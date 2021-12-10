## BEGIN TERRAFORM GENERATED CONTENT
${ipa.network_interface.0.access_config.0.nat_ip} ${ipa.name}.c.${project_id}.internal ${ipa.name}

%{ for node in datanode ~}
${node.network_interface.0.access_config.0.nat_ip} ${node.name}.c.${project_id}.internal ${node.name}
%{ endfor ~}

%{ for node in namenode ~}
${node.network_interface.0.access_config.0.nat_ip} ${node.name}.c.${project_id}.internal ${node.name}
%{ endfor ~}
## END TERRAFORM GENERATED CONTENT
