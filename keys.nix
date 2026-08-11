rec {
  # Tailscale MagicDNS hostnames per environment. Referenced by
  # infra/default.nix (local tooling) and the deploy workflows.
  tailscaleHost = {
    prod = "st0x-liquidity-nixos";
    staging = "st0x-liquidity-staging";
  };

  keys = {
    # purpose: dev/ops
    juan = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIHHNeV3nfiJS0QE2JoW3d0dRw1j6OVKl7rXor24XHvsd";
    # purpose: op sec auditing and manual cli while fixing gaps in automation
    alastair = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIJArH3PA+bFIon0JkCVQGs9aWr45lnVjiiTLLO9BPItn";
    jakub = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIM8D2RLBbEfB0GZKhT7tlT43pkKbrMnOR7cZfXoG9NbH";
    alex = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIA9MytndYpB/zpdh4kCXE0KF+NoLrpe+ij+zctM3xUCf";

    # purpose: deployments from github actions
    ci = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIARWUchVuQvkFm2tzspdC79hhRyYbWzRjs5iimhxewUy";
    # purpose: initial key for Digital Ocean to include when first creating
    # the instance and it's matched by name in their dashboard
    st0x-op = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIEsSQPtobcSZM/Rn/aizWD+aCaWVzuAZInNiCf/34oR+";

    # purpose: used by the remote host to decrypt ragenix secrets we need for
    # the system to work at all. auto-generated during bootstrap when
    # nixos-anywhere turns the initially ubuntu instance into nixos
    host-prod = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIDXNYu7AGEwlInGoiqcPIF7e46rcbipk+as9UWCYctxh";
    # auto-generated during bootstrap
    host-staging = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIDHGyaEKFAUbg2Lo2w3sjFAh19DQpGJCalp538SgXeu3";
  };

  roles = with keys; {
    # access to terraform state and encrypted vars (shared across environments)
    infra = [
      st0x-op
      juan
      alastair
      ci
    ];

    prod = {
      ssh = [
        juan
        alastair
        jakub
        alex
        st0x-op
        ci
      ];
      service = [
        st0x-op
        juan
        alastair
        jakub
        alex
        host-prod
      ];
    };

    staging = {
      ssh = [
        juan
        alastair
        jakub
        alex
        st0x-op
        ci
      ];
      service = [
        st0x-op
        juan
        alastair
        jakub
        alex
        host-staging
      ];
    };
  };

}
