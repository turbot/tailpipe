package plugin_manager

import (
	"github.com/hashicorp/go-plugin"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/shared"
)

// PluginClient is the client object used by clients of the plugin
type PluginClient struct {
	shared.TailpipePluginClientWrapper
	Name   string
	client *plugin.Client
	schema map[string]string
}

func NewPluginClient(client *plugin.Client, pluginName string) (*PluginClient, error) {
	// connect via GRPC
	rpcClient, err := client.Client()
	if err != nil {
		return nil, err
	}

	// request the plugin
	raw, err := rpcClient.Dispense(pluginName)
	if err != nil {
		return nil, err
	}
	// we should have a stub plugin now

	res := &PluginClient{
		TailpipePluginClientWrapper: *(raw.(*shared.TailpipePluginClientWrapper)),
		Name:                        pluginName,
		client:                      client,
	}
	return res, nil
}

// Exited returned whether the underlying client has exited, i.e. the plugin has terminated
func (c *PluginClient) Exited() bool {
	return c.client.Exited()
}
