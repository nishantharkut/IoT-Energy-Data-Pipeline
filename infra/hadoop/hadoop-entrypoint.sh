#!/usr/bin/env bash
set -euo pipefail

mode="${1:-}"
case "$mode" in
  hdfs)
    if [ ! -f /var/lib/hadoop-hdfs/name/current/VERSION ]; then
      /opt/hadoop/bin/hdfs namenode -format -force -nonInteractive
    fi
    /opt/hadoop/bin/hdfs --daemon start datanode
    exec /opt/hadoop/bin/hdfs namenode
    ;;
  yarn)
    until /opt/hadoop/bin/hdfs dfs -ls / >/dev/null 2>&1; do
      sleep 2
    done
    /opt/hadoop/bin/yarn --daemon start nodemanager
    /opt/hadoop/bin/mapred --daemon start historyserver
    exec /opt/hadoop/bin/yarn resourcemanager
    ;;
  *)
    exec "$@"
    ;;
esac
