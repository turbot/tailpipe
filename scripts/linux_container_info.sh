#!/bin/sh
# This is a script to get the information about the linux container.
# Used in release smoke tests.

uname -a # uname information
cat /etc/os-release # OS version information
ldd --version # glibc version information 