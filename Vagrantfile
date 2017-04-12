Vagrant.configure(2) do |config|
  config.vm.box = "ubuntu/xenial64"
  config.vm.network :forwarded_port, guest: 3000, host: 50000 # HTTP
  config.vm.network :forwarded_port, guest: 3443, host: 50001 # HTTPS

  config.vm.provision :shell, path: "Vagrant.bootstrap.sh"
end