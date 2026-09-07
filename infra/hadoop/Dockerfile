FROM apache/hadoop:3.4.3@sha256:127774dadab40ce84df7ac668a7a8c99945688b3fe336f1388f4477ca33e1529

USER root

RUN if command -v apt-get >/dev/null 2>&1; then \
      apt-get update && apt-get install -y --no-install-recommends python3 && \
      rm -rf /var/lib/apt/lists/*; \
    elif command -v dnf >/dev/null 2>&1; then \
      dnf install -y python3 && dnf clean all; \
    elif command -v yum >/dev/null 2>&1; then \
      yum install -y python3 && yum clean all; \
    else \
      echo "No supported package manager for Python 3" >&2; exit 1; \
    fi

COPY infra/hadoop/conf/ /opt/hadoop/etc/hadoop/
COPY infra/hadoop/hadoop-entrypoint.sh /usr/local/bin/hadoop-entrypoint
COPY jobs/hadoop/mapper.py jobs/hadoop/reducer.py \
     jobs/hadoop/record_mapper.py jobs/hadoop/record_reducer.py \
     jobs/hadoop/distributed_oracle.py /opt/pipeline/jobs/hadoop/

RUN chmod 0755 /usr/local/bin/hadoop-entrypoint && \
    mkdir -p /var/lib/hadoop-hdfs/name /var/lib/hadoop-hdfs/data /opt/hadoop/logs

ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ENV HDFS_NAMENODE_USER=root
ENV HDFS_DATANODE_USER=root
ENV YARN_RESOURCEMANAGER_USER=root
ENV YARN_NODEMANAGER_USER=root
ENV MAPRED_HISTORYSERVER_USER=root

ENTRYPOINT ["/usr/local/bin/hadoop-entrypoint"]
