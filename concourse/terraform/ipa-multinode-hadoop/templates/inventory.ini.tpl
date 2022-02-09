[ipa]
${ipa.name}

[active_namenode]
${namenode.0.name}

[standby_namenode]
${namenode.1.name}

[namenode:children]
active_namenode
standby_namenode

[datanode]
%{ for node in datanode ~}
${node.name}
%{ endfor ~}

[hdfs:children]
namenode
datanode

[hive]
${namenode.1.name}