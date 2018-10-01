#!/usr/bin/env bash
ROLE=$(/usr/share/google/get_metadata_value attributes/dataproc-role)
if [[ "${ROLE}" == 'Master' ]]; then
  echo "DO NOTHING"
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