# Migrate existing root-level resources into module.prod
moved {
  from = digitalocean_droplet.nixos
  to   = module.prod.digitalocean_droplet.nixos
}
moved {
  from = digitalocean_volume.data
  to   = module.prod.digitalocean_volume.data
}
moved {
  from = digitalocean_volume_attachment.data
  to   = module.prod.digitalocean_volume_attachment.data
}
moved {
  from = digitalocean_reserved_ip.nixos
  to   = module.prod.digitalocean_reserved_ip.nixos
}
moved {
  from = digitalocean_reserved_ip_assignment.nixos
  to   = module.prod.digitalocean_reserved_ip_assignment.nixos
}
moved {
  from = digitalocean_firewall.st0x
  to   = module.prod.digitalocean_firewall.st0x
}

module "prod" {
  source = "./modules/stack"

  environment  = "prod"
  do_token     = var.do_token
  droplet_size = var.prod_droplet_size
  droplet_name       = "st0x-liquidity-nixos"
  volume_name        = "st0x-liquidity-data"
  volume_description = "Persistent storage for SQLite databases and logs"
}

module "staging" {
  source = "./modules/stack"

  environment  = "staging"
  do_token     = var.do_token
  droplet_size = var.staging_droplet_size
}
