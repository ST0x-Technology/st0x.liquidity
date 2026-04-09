variable "environment" {
  description = "Environment name (prod, staging)"
  type        = string

  validation {
    condition     = contains(["prod", "staging"], var.environment)
    error_message = "environment must be one of: prod, staging"
  }
}

variable "do_token" {
  description = "DigitalOcean API token"
  type        = string
  sensitive   = true
}

variable "ssh_key_name" {
  description = "Name of the SSH key in DigitalOcean to add to the droplet"
  type        = string
  default     = "st0x-op"
}

variable "region" {
  description = "DigitalOcean region"
  type        = string
  default     = "nyc3"
}

variable "droplet_size" {
  description = "Droplet size slug"
  type        = string

  validation {
    condition     = length(trimspace(var.droplet_size)) > 0
    error_message = "droplet_size must not be empty"
  }
}

variable "volume_size_gb" {
  description = "Block storage volume size in GB"
  type        = number
  default     = 5

  validation {
    condition     = var.volume_size_gb >= 1
    error_message = "volume_size_gb must be at least 1 GB"
  }
}

variable "droplet_name" {
  description = "Override droplet name (defaults to st0x-liquidity-{environment})"
  type        = string
  default     = null
}

variable "volume_name" {
  description = "Override volume name (defaults to st0x-liquidity-{environment}-data)"
  type        = string
  default     = null
}

variable "volume_description" {
  description = "Override volume description (defaults to include environment)"
  type        = string
  default     = null
}
