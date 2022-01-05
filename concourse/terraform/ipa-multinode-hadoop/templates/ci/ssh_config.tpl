Host ${ipa.name}
	Hostname ${ipa.network_interface.0.network_ip}
	User ipa
	IdentityFile ~/.ssh/${cluster_name}
	UserKnownHostsFile /dev/null
	StrictHostKeyChecking no

%{ for node in namenode ~}
Host ${node.name}
	Hostname ${node.network_interface.0.network_ip}
	User hdfs
	IdentityFile ~/.ssh/${cluster_name}
	UserKnownHostsFile /dev/null
	StrictHostKeyChecking no

%{ endfor ~}

%{ for node in datanode ~}
Host ${node.name}
	Hostname ${node.network_interface.0.network_ip}
	User hdfs
	IdentityFile ~/.ssh/${cluster_name}
	UserKnownHostsFile /dev/null
	StrictHostKeyChecking no

%{ endfor ~}
