#!/bin/bash
ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
if [[ "${ROLE}" == 'Master' ]]; then
  hadoop distcp gs://data-gpdb-ud-tpch/10/lineitem_data/*.tbl /tmp/lineitem_read/
  hadoop fs -cp /tmp/lineitem_read /tmp/lineitem_read_gphdfs
fi

cat > /tmp/core-site-patch.xml <<-EOF
  <property>
    <name>hadoop.proxyuser.gpadmin.hosts</name>
    <value>*</value>
  </property>
  <property>
    <name>hadoop.proxyuser.gpadmin.groups</name>
    <value>*</value>
  </property>
EOF

sed -i -e '/<configuration>/r /tmp/core-site-patch.xml' \
     /etc/hadoop/conf/core-site.xml
rm -rf /tmp/core-site-patch.xml
