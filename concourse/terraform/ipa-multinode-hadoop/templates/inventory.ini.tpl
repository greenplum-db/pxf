[ipa]
${ipa.name}

[namenode]
%{ for node in namenode ~}
${node.name}
%{ endfor ~}

[datanode]
%{ for node in datanode ~}
${node.name}
%{ endfor ~}

[hdfs:children]
namenode
datanode
