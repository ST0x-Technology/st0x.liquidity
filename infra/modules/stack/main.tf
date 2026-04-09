terraform {
  required_providers {
    digitalocean = {
      source  = "digitalocean/digitalocean"
      version = ">= 2.40"
    }
  }
}

data "digitalocean_ssh_key" "deploy" {
  name = var.ssh_key_name
}

locals {
  droplet_name = coalesce(var.droplet_name, "st0x-liquidity-${var.environment}")
  volume_name  = coalesce(var.volume_name, "st0x-liquidity-${var.environment}-data")
}

resource "digitalocean_volume" "data" {
  region                  = var.region
  name                    = local.volume_name
  size                    = var.volume_size_gb
  initial_filesystem_type = "ext4"
  description             = coalesce(var.volume_description, "Persistent storage for SQLite databases and logs (${var.environment})")

  lifecycle {
    prevent_destroy = true
  }
}

resource "digitalocean_droplet" "nixos" {
  image    = "ubuntu-24-04-x64"
  name     = local.droplet_name
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

resource "digitalocean_firewall" "st0x" {
  name        = "st0x-liquidity-${var.environment}"
  droplet_ids = [digitalocean_droplet.nixos.id]

  # SSH -- needed for bootstrap (nixos-anywhere) and deploy-rs
  inbound_rule {
    protocol         = "tcp"
    port_range       = "22"
    source_addresses = ["0.0.0.0/0", "::/0"]
  }

  # Tailscale WireGuard -- must be publicly reachable for NAT traversal.
  # Authentication is handled by WireGuard's Noise protocol, not by IP filtering.
  inbound_rule {
    protocol         = "udp"
    port_range       = "41641"
    source_addresses = ["0.0.0.0/0", "::/0"]
  }

  # All outbound
  outbound_rule {
    protocol              = "tcp"
    port_range            = "1-65535"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }

  outbound_rule {
    protocol              = "udp"
    port_range            = "1-65535"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }

  outbound_rule {
    protocol              = "icmp"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }
}
