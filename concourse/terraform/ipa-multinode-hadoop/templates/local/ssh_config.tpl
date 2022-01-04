Host ${ipa.name}
	Hostname ${ipa.network_interface.0.access_config.0.nat_ip}
	User ipa
	IdentityFile ~/.ssh/ipa_${cluster_name}_rsa
	UserKnownHostsFile /dev/null
	StrictHostKeyChecking no

%{ for node in namenode ~}
Host ${node.name}
	Hostname ${node.network_interface.0.access_config.0.nat_ip}
	User hdfs
	IdentityFile ~/.ssh/ipa_${cluster_name}_rsa
	UserKnownHostsFile /dev/null
	StrictHostKeyChecking no

%{ endfor ~}

%{ for node in datanode ~}
Host ${node.name}
	Hostname ${node.network_interface.0.access_config.0.nat_ip}
	User hdfs
	IdentityFile ~/.ssh/ipa_${cluster_name}_rsa
	UserKnownHostsFile /dev/null
	StrictHostKeyChecking no

%{ endfor ~}
