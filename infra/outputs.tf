# Prod
output "prod_droplet_id" {
  description = "ID of the prod NixOS droplet"
  value       = module.prod.droplet_id
}

output "prod_droplet_ipv4" {
  description = "Public IPv4 address of the prod droplet"
  value       = module.prod.droplet_ipv4
}

output "prod_reserved_ip" {
  description = "Reserved IP address assigned to the prod droplet"
  value       = module.prod.reserved_ip
}

output "prod_volume_id" {
  description = "ID of the prod data volume"
  value       = module.prod.volume_id
}

# Staging
output "staging_droplet_id" {
  description = "ID of the staging NixOS droplet"
  value       = module.staging.droplet_id
}

output "staging_droplet_ipv4" {
  description = "Public IPv4 address of the staging droplet"
  value       = module.staging.droplet_ipv4
}

output "staging_reserved_ip" {
  description = "Reserved IP address assigned to the staging droplet"
  value       = module.staging.reserved_ip
}

output "staging_volume_id" {
  description = "ID of the staging data volume"
  value       = module.staging.volume_id
}
