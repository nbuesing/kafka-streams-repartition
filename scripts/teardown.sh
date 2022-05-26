#!/bin/sh

alias dc='docker compose'

(cd cluster; dc down -v)

#rm -fr applications/stores/analytics_sliding

#docker network rm ksd
