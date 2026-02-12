data "digitalocean_ssh_key" "deploy" {
  name = var.ssh_key_name
}

resource "digitalocean_volume" "data" {
  region                  = var.region
  name                    = "st0x-liquidity-data"
  size                    = var.volume_size_gb
  initial_filesystem_type = "ext4"
  description             = "Persistent storage for SQLite databases and logs"
}

resource "digitalocean_droplet" "nixos" {
  image    = "ubuntu-24-04-x64"
  name     = "st0x-liquidity-nixos"
  region   = var.region
  size     = var.droplet_size
  ssh_keys = [data.digitalocean_ssh_key.deploy.id]
}

resource "digitalocean_volume_attachment" "data" {
  droplet_id = digitalocean_droplet.nixos.id
  volume_id  = digitalocean_volume.data.id
}

resource "digitalocean_reserved_ip" "nixos" {
  region = var.region
}

resource "digitalocean_reserved_ip_assignment" "nixos" {
  ip_address = digitalocean_reserved_ip.nixos.ip_address
  droplet_id = digitalocean_droplet.nixos.id
}