package interactive

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"

	"github.com/alecthomas/chroma/formatters"
	"github.com/alecthomas/chroma/lexers"
	"github.com/alecthomas/chroma/styles"
	"github.com/c-bata/go-prompt"
	"github.com/spf13/viper"
	"github.com/turbot/go-kit/helpers"
	pconstants "github.com/turbot/pipe-fittings/constants"
	"github.com/turbot/pipe-fittings/error_helpers"
	"github.com/turbot/pipe-fittings/modconfig"
	"github.com/turbot/pipe-fittings/utils"

	"github.com/turbot/tailpipe/internal/metaquery"
	"github.com/turbot/tailpipe/internal/query"
	"github.com/turbot/tailpipe/internal/statushooks"
)

type AfterPromptCloseAction int

const (
	AfterPromptCloseExit AfterPromptCloseAction = iota
	AfterPromptCloseRestart
)

// InteractiveClient is a wrapper over a LocalClient and a Prompt to facilitate interactive query prompt
type InteractiveClient struct {
	promptResult            *RunInteractivePromptResult
	interactiveBuffer       []string
	interactivePrompt       *prompt.Prompt
	interactiveQueryHistory *QueryHistory
	autocompleteOnEmpty     bool
	// the cancellation function for the active query - may be nil
	// NOTE: should ONLY be called by cancelActiveQueryIfAny
	cancelActiveQuery context.CancelFunc
	cancelPrompt      context.CancelFunc

	afterClose AfterPromptCloseAction
	// lock while execution is occurring to avoid errors/warnings being shown
	executionLock sync.Mutex
	// the schema metadata - this is loaded asynchronously during init
	//schemaMetadata *db_common.SchemaMetadata
	highlighter *Highlighter
	// hidePrompt is used to render a blank as the prompt prefix
	hidePrompt bool

	suggestions *autoCompleteSuggestions
}

func getHighlighter(theme string) *Highlighter {
	return newHighlighter(
		lexers.Get("sql"),
		formatters.Get("terminal256"),
		styles.Native,
	)
}

func newInteractiveClient(ctx context.Context, result *RunInteractivePromptResult) (*InteractiveClient, error) {
	interactiveQueryHistory, err := newQueryHistory()
	if err != nil {
		return nil, err
	}
	c := &InteractiveClient{
		promptResult:            result,
		interactiveQueryHistory: interactiveQueryHistory,
		interactiveBuffer:       []string{},
		autocompleteOnEmpty:     false,
		highlighter:             getHighlighter(viper.GetString(pconstants.ArgTheme)),
		suggestions:             newAutocompleteSuggestions(),
	}

	// initialise autocomplete suggestions
	//nolint:golint,errcheck // worst case is we won't have autocomplete - this is not a failure
	c.initialiseSuggestions(ctx)

	return c, nil
}

// InteractivePrompt starts an interactive prompt and return
func (c *InteractiveClient) InteractivePrompt(parentContext context.Context) {
	// start a cancel handler for the interactive client - this will call activeQueryCancelFunc if it is set
	// (registered when we call createQueryContext)
	quitChannel := c.startCancelHandler()

	// create a cancel context for the prompt - this will set c.cancelPrompt
	ctx := c.createPromptContext(parentContext)

	defer func() {
		if r := recover(); r != nil {
			error_helpers.ShowError(ctx, helpers.ToError(r))
		}
		// close up the SIGINT channel so that the receiver goroutine can quit
		quitChannel <- true
		close(quitChannel)

		// close the result stream
		// this needs to be the last thing we do,
		// as the query result display code will exit once the result stream is closed
		c.promptResult.Streamer.Close()
	}()

	statushooks.Message(
		ctx,
		fmt.Sprintf("Welcome to Tailpipe v%s", viper.GetString("main.version")),
		fmt.Sprintf("For more information, type %s", pconstants.Bold(".help")),
	)

	// run the prompt in a goroutine, so we can also detect async initialisation errors
	promptResultChan := make(chan struct{}, 1)
	c.runInteractivePromptAsync(ctx, promptResultChan)

	// select results
	for {
		select {

		case <-promptResultChan:
			// persist saved history
			//nolint:golint,errcheck // worst case is history is not persisted - not a failure
			c.interactiveQueryHistory.Persist()
			// check post-close action
			if c.afterClose == AfterPromptCloseExit {
				// clear prompt so any messages/warnings can be displayed without the prompt
				c.hidePrompt = true
				c.interactivePrompt.ClearLine()
				return
			}
			// create new context with a cancellation func
			ctx = c.createPromptContext(parentContext)
			// now run it again
			c.runInteractivePromptAsync(ctx, promptResultChan)
		}
	}
}

// ClosePrompt cancels the running prompt, setting the action to take after close
func (c *InteractiveClient) ClosePrompt(afterClose AfterPromptCloseAction) {
	c.afterClose = afterClose
	c.cancelPrompt()
}

// retrieve both the raw query result and a sanitised version in list form
func (c *InteractiveClient) loadSchema() error {
	utils.LogTime("db_client.loadSchema start")
	defer utils.LogTime("db_client.loadSchema end")

	// TODO #interactive load schema
	//c.schemaMetadata = metadata

	return nil
}

func (c *InteractiveClient) runInteractivePromptAsync(ctx context.Context, promptResultChan chan struct{}) {
	go func() {
		c.runInteractivePrompt(ctx)
		promptResultChan <- struct{}{}
	}()
}

func (c *InteractiveClient) runInteractivePrompt(ctx context.Context) {
	defer func() {
		// this is to catch the PANIC that gets raised by
		// the executor of go-prompt
		//
		// We need to do it this way, since there is no
		// clean way to reload go-prompt so that we can
		// populate the history stack
		//
		if r := recover(); r != nil {
			// show the panic and restart the prompt
			error_helpers.ShowError(ctx, helpers.ToError(r))
			c.afterClose = AfterPromptCloseRestart
			c.hidePrompt = false
			return
		}
	}()

	callExecutor := func(line string) {
		c.executor(ctx, line)
	}
	completer := func(d prompt.Document) []prompt.Suggest {
		return c.queryCompleter(d)
	}
	c.interactivePrompt = prompt.New(
		callExecutor,
		completer,
		prompt.OptionTitle("tailpipe interactive client "),
		prompt.OptionLivePrefix(func() (prefix string, useLive bool) {
			prefix = "> "
			useLive = true
			if len(c.interactiveBuffer) > 0 {
				prefix = ">>  "
			}
			if c.hidePrompt {
				prefix = ""
			}
			return
		}),
		prompt.OptionFormatter(c.highlighter.Highlight),
		prompt.OptionHistory(c.interactiveQueryHistory.Get()),
		prompt.OptionInputTextColor(prompt.DefaultColor),
		prompt.OptionPrefixTextColor(prompt.DefaultColor),
		prompt.OptionMaxSuggestion(20),
		// Known Key Bindings
		prompt.OptionAddKeyBind(prompt.KeyBind{
			Key: prompt.ControlC,
			Fn:  func(b *prompt.Buffer) { c.breakMultilinePrompt(b) },
		}),
		prompt.OptionAddKeyBind(prompt.KeyBind{
			Key: prompt.ControlD,
			Fn: func(b *prompt.Buffer) {
				if b.Text() == "" {
					c.ClosePrompt(AfterPromptCloseExit)
				}
			},
		}),
		prompt.OptionAddKeyBind(prompt.KeyBind{
			Key: prompt.Tab,
			Fn: func(b *prompt.Buffer) {
				if len(b.Text()) == 0 {
					c.autocompleteOnEmpty = true
				} else {
					c.autocompleteOnEmpty = false
				}
			},
		}),
		prompt.OptionAddKeyBind(prompt.KeyBind{
			Key: prompt.Escape,
			Fn: func(b *prompt.Buffer) {
				if len(b.Text()) == 0 {
					c.autocompleteOnEmpty = false
				}
			},
		}),
		prompt.OptionAddKeyBind(prompt.KeyBind{
			Key: prompt.ShiftLeft,
			Fn:  prompt.GoLeftChar,
		}),
		prompt.OptionAddKeyBind(prompt.KeyBind{
			Key: prompt.ShiftRight,
			Fn:  prompt.GoRightChar,
		}),
		prompt.OptionAddKeyBind(prompt.KeyBind{
			Key: prompt.ShiftUp,
			Fn:  func(b *prompt.Buffer) { /*ignore*/ },
		}),
		prompt.OptionAddKeyBind(prompt.KeyBind{
			Key: prompt.ShiftDown,
			Fn:  func(b *prompt.Buffer) { /*ignore*/ },
		}),
		// Opt+LeftArrow
		prompt.OptionAddASCIICodeBind(prompt.ASCIICodeBind{
			ASCIICode: pconstants.OptLeftArrowASCIICode,
			Fn:        prompt.GoLeftWord,
		}),
		// Opt+RightArrow
		prompt.OptionAddASCIICodeBind(prompt.ASCIICodeBind{
			ASCIICode: pconstants.OptRightArrowASCIICode,
			Fn:        prompt.GoRightWord,
		}),
		// Alt+LeftArrow
		prompt.OptionAddASCIICodeBind(prompt.ASCIICodeBind{
			ASCIICode: pconstants.AltLeftArrowASCIICode,
			Fn:        prompt.GoLeftWord,
		}),
		// Alt+RightArrow
		prompt.OptionAddASCIICodeBind(prompt.ASCIICodeBind{
			ASCIICode: pconstants.AltRightArrowASCIICode,
			Fn:        prompt.GoRightWord,
		}),
		prompt.OptionBufferPreHook(func(input string) (modifiedInput string, ignore bool) {
			// if this is not WSL, return as-is
			if !utils.IsWSL() {
				return input, false
			}
			return cleanBufferForWSL(input)
		}),
	)
	// set this to a default
	c.autocompleteOnEmpty = false
	c.interactivePrompt.RunCtx(ctx)

	return
}

func cleanBufferForWSL(s string) (string, bool) {
	b := []byte(s)
	// in WSL, 'Alt' combo-characters are denoted by [27, ASCII of character]
	// if we get a combination which has 27 as prefix - we should ignore it
	// this is inline with other interactive clients like pgcli
	if len(b) > 1 && bytes.HasPrefix(b, []byte{byte(27)}) {
		// ignore it
		return "", true
	}
	return string(b), false
}

func (c *InteractiveClient) breakMultilinePrompt(buffer *prompt.Buffer) {
	c.interactiveBuffer = []string{}
}

func (c *InteractiveClient) executor(ctx context.Context, line string) {
	// take an execution lock, so that errors and warnings don't show up while
	// we are underway
	c.executionLock.Lock()
	defer c.executionLock.Unlock()

	// set afterClose to restart - is we are exiting the metaquery will set this to AfterPromptCloseExit
	c.afterClose = AfterPromptCloseRestart

	line = strings.TrimSpace(line)

	resolvedQuery := c.getQuery(ctx, line)
	if resolvedQuery == nil {
		// we failed to resolve a query, or are in the middle of a multi-line entry
		// restart the prompt, DO NOT clear the interactive buffer
		c.restartInteractiveSession()
		return
	}

	// we successfully retrieved a query

	// create a  context for the execution of the query
	queryCtx := c.createQueryContext(ctx)

	if resolvedQuery.IsMetaQuery {
		c.hidePrompt = true
		c.interactivePrompt.Render()

		if err := c.executeMetaquery(queryCtx, resolvedQuery.ExecuteSQL); err != nil {
			error_helpers.ShowError(ctx, err)
		}
		c.hidePrompt = false

		// cancel the context
		c.cancelActiveQueryIfAny()
	} else {
		statushooks.Show(ctx)
		defer statushooks.Done(ctx)
		statushooks.SetStatus(ctx, "Executing query…")
		// otherwise execute query
		c.executeQuery(ctx, queryCtx, resolvedQuery)
	}

	// restart the prompt
	c.restartInteractiveSession()
}

func (c *InteractiveClient) executeQuery(ctx context.Context, queryCtx context.Context, resolvedQuery *modconfig.ResolvedQuery) {

	// TODO #interactive
	//t := time.Now()
	//result, err := c.client().Execute(queryCtx, resolvedQuery.ExecuteSQL, resolvedQuery.Args...)
	//if err != nil {
	//	error_helpers.ShowError(ctx, error_helpers.HandleCancelError(err))
	//	// if timing flag is enabled, show the time taken for the query to fail
	//	if viper.GetString(pconstants.ArgTiming) != pconstants.ArgOff {
	//		display.DisplayErrorTiming(t)
	//	}
	//} else {
	//	c.promptResult.Streamer.StreamResult(result)
	//}
}

func (c *InteractiveClient) getQuery(ctx context.Context, line string) *modconfig.ResolvedQuery {
	// if it's an empty line, then we don't need to do anything
	if line == "" {
		return nil
	}

	// store the history (the raw line which was entered)
	historyEntry := line
	defer func() {
		if len(historyEntry) > 0 {
			// we want to store even if we fail to resolve a query
			c.interactiveQueryHistory.Push(historyEntry)
		}

	}()

	// push the current line into the buffer
	c.interactiveBuffer = append(c.interactiveBuffer, line)

	// expand the buffer out into 'queryString'
	queryString := strings.Join(c.interactiveBuffer, "\n")

	// check if the contents in the buffer evaluates to a metaquery
	if metaquery.IsMetaQuery(line) {
		// this is a metaquery
		// clear the interactive buffer
		c.interactiveBuffer = nil
		return &modconfig.ResolvedQuery{
			ExecuteSQL:  line,
			IsMetaQuery: true,
		}
	}

	// in case of a named query call with params, parse the where clause
	resolvedQuery, err := query.ResolveQueryAndArgsFromSQLString(queryString)
	if err != nil {
		// if we fail to resolve:
		// - show error but do not return it so we  stay in the prompt
		// - do not clear history item - we want to store bad entry in history
		// - clear interactive buffer
		c.interactiveBuffer = nil
		error_helpers.ShowError(ctx, err)
		return nil
	}

	// should we execute?
	// we will NOT execute if we are in multiline mode, there is no semi-colon
	// and it is NOT a metaquery or a named query
	if !c.shouldExecute(queryString) {
		// is we are not executing, do not store history
		historyEntry = ""
		// do not clear interactive buffer
		return nil
	}

	// so we need to execute
	// clear the interactive buffer
	c.interactiveBuffer = nil

	// what are we executing?

	// if the line is ONLY a semicolon, do nothing and restart interactive session
	if strings.TrimSpace(resolvedQuery.ExecuteSQL) == ";" {
		// do not store in history
		historyEntry = ""
		c.restartInteractiveSession()
		return nil
	}
	// if this is a multiline query, update history entry
	if len(strings.Split(resolvedQuery.ExecuteSQL, "\n")) > 1 {
		historyEntry = resolvedQuery.ExecuteSQL
	}

	return resolvedQuery
}

func (c *InteractiveClient) executeMetaquery(ctx context.Context, query string) error {
	// validate the metaquery arguments
	validateResult := metaquery.Validate(query)
	if validateResult.Message != "" {
		fmt.Println(validateResult.Message)
	}
	if err := validateResult.Err; err != nil {
		return err
	}
	if !validateResult.ShouldRun {
		return nil
	}

	// validation passed, now we will run
	return metaquery.Handle(ctx, &metaquery.HandlerInput{
		Query:       query,
		Prompt:      c.interactivePrompt,
		ClosePrompt: func() { c.afterClose = AfterPromptCloseExit },
	})
}

func (c *InteractiveClient) restartInteractiveSession() {
	// restart the prompt
	c.ClosePrompt(c.afterClose)
}

func (c *InteractiveClient) shouldExecute(line string) bool {
	if !viper.GetBool(pconstants.ArgMultiLine) {
		// NOT multiline mode
		return true
	}
	if metaquery.IsMetaQuery(line) {
		// execute metaqueries with no ';' even in multiline mode
		return true
	}
	if strings.HasSuffix(line, ";") {
		// statement has terminating ';'
		return true
	}

	return false
}

func (c *InteractiveClient) queryCompleter(d prompt.Document) []prompt.Suggest {
	if !viper.GetBool(pconstants.ArgAutoComplete) {
		return nil
	}

	text := strings.TrimLeft(strings.ToLower(d.CurrentLine()), " ")
	if len(text) == 0 && !c.autocompleteOnEmpty {
		// if nothing has been typed yet, no point
		// giving suggestions
		return nil
	}

	var s []prompt.Suggest

	switch {
	case isFirstWord(text):
		suggestions := c.getFirstWordSuggestions(text)
		s = append(s, suggestions...)
	case metaquery.IsMetaQuery(text):
		suggestions := metaquery.Complete(&metaquery.CompleterInput{
			Query:            text,
			TableSuggestions: c.getTableAndConnectionSuggestions(lastWord(text)),
		})
		s = append(s, suggestions...)
	default:
		if queryInfo := getQueryInfo(text); queryInfo.EditingTable {
			tableSuggestions := c.getTableAndConnectionSuggestions(lastWord(text))
			s = append(s, tableSuggestions...)
		}
	}

	return prompt.FilterHasPrefix(s, d.GetWordBeforeCursor(), true)
}

func (c *InteractiveClient) getFirstWordSuggestions(word string) []prompt.Suggest {
	var querySuggestions []prompt.Suggest
	// if this a qualified query try to extract connection
	parts := strings.Split(word, ".")
	if len(parts) > 1 {
		// if first word is a mod name we know about, return appropriate suggestions
		modName := strings.TrimSpace(parts[0])
		if modQueries, isMod := c.suggestions.queriesByMod[modName]; isMod {
			querySuggestions = modQueries
		} else {
			//  otherwise return mods names and unqualified queries
			//nolint:golint,gocritic // we want this to go into a different slice
			querySuggestions = append(c.suggestions.mods, c.suggestions.unqualifiedQueries...)
		}
	}

	var s []prompt.Suggest
	// add all we know that can be the first words
	// named queries
	s = append(s, querySuggestions...)
	// "select", "with"
	s = append(s, prompt.Suggest{Text: "select", Output: "select"}, prompt.Suggest{Text: "with", Output: "with"})
	// metaqueries
	s = append(s, metaquery.PromptSuggestions()...)
	return s
}

func (c *InteractiveClient) getTableAndConnectionSuggestions(word string) []prompt.Suggest {
	// try to extract connection
	parts := strings.SplitN(word, ".", 2)
	if len(parts) == 1 {
		// no connection, just return schemas and unqualified tables
		return append(c.suggestions.schemas, c.suggestions.unqualifiedTables...)
	}

	connection := strings.TrimSpace(parts[0])
	t := c.suggestions.tablesBySchema[connection]
	return t
}

func (c *InteractiveClient) newSuggestion(itemType string, description string, name string) prompt.Suggest {
	if description != "" {
		itemType += fmt.Sprintf(": %s", description)
	}
	return prompt.Suggest{Text: name, Output: name, Description: itemType}
}

func (c *InteractiveClient) startCancelHandler() chan bool {
	sigIntChannel := make(chan os.Signal, 1)
	quitChannel := make(chan bool, 1)
	signal.Notify(sigIntChannel, os.Interrupt)
	go func() {
		for {
			select {
			case <-sigIntChannel:
				log.Println("[INFO] interactive client cancel handler got SIGINT")
				//  call cancelActiveQueryIfAny which the for the active query, if there is one
				c.cancelActiveQueryIfAny()
				// keep waiting for further cancellations
			case <-quitChannel:
				log.Println("[INFO] cancel handler exiting")
				c.cancelActiveQueryIfAny()
				// we're done
				return
			}
		}
	}()
	return quitChannel
}

func (c *InteractiveClient) showMessages(ctx context.Context, showMessages func()) {
	statushooks.Done(ctx)
	// clear the prompt
	// NOTE: this must be done BEFORE setting hidePrompt
	// otherwise the cursor calculations in go-prompt do not work and multi-line test is not cleared
	c.interactivePrompt.ClearLine()
	// set the flag hide the prompt prefix in the next prompt render cycle
	c.hidePrompt = true
	// call ClearLine to render the empty prefix
	c.interactivePrompt.ClearLine()

	// call the passed in func to display the messages
	showMessages()

	// show the prompt again
	c.hidePrompt = false

	// We need to render the prompt here to make sure that it comes back
	// after the messages have been displayed (only if there's no execution)
	//
	// We check for query execution by TRYING to acquire the same lock that
	// execution locks on
	//
	// If we can acquire a lock, that means that there's no
	// query execution underway - and it is safe to render the prompt
	//
	// otherwise, that query execution is waiting for this init to finish
	// and as such will be out of the prompt - in which case, we shouldn't
	// re-render the prompt
	//
	// the prompt will be re-rendered when the query execution finished
	if c.executionLock.TryLock() {
		c.interactivePrompt.Render()
		// release the lock
		c.executionLock.Unlock()
	}
}
