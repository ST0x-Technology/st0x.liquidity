rec {
  # Tailscale MagicDNS hostname for the production server.
  # Referenced by infra/default.nix and .github/workflows/ci.yaml.
  tailscaleHost = "st0x-liquidity-nixos";

  keys = {
    # purpose: dev/ops
    gleb =
      "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIHepyxN9hvXzbCY/z0amzldy7DXjNdyetnVaQexRgDEX";
    # purpose: dev/ops
    juan =
      "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIHHNeV3nfiJS0QE2JoW3d0dRw1j6OVKl7rXor24XHvsd";
    # purpose: op sec auditing and manual cli while fixing gaps in automation
    alastair =
      "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIJArH3PA+bFIon0JkCVQGs9aWr45lnVjiiTLLO9BPItn";

    # purpose: deployments from github actions
    ci =
      "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIARWUchVuQvkFm2tzspdC79hhRyYbWzRjs5iimhxewUy";
    # purpose: initial key for Digital Ocean to include when first creating
    # the instance and it's matched by name in their dashboard
    st0x-op =
      "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIPZ56nOYbGDd0ZfbqxeY7AbvaQGQrHnlC80ccpRGpCoj";

    # purpoose: used by the remote host to decrypt ragenix secrets we need for the
    # system to work at all. it gets auto-generated during the bootstrap phase of
    # our infra provisioning process when nixos-anywhere turns the initially ubuntu
    # instance into a nixos one. runs once after server provisioning
    host =
      "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIDXNYu7AGEwlInGoiqcPIF7e46rcbipk+as9UWCYctxh";
  };

  roles = with keys; {
    # this is for both accessing terraform secrets and encrypted state
    # but the only reason you need one without the other is because ssh
    # requires decoding terraform state just to get the host ip address
    infra = [ st0x-op ci juan ];

    # this role was meant to be for the ability to ssh into the machine
    # but in the future we should restrict what that exactly means in
    # terms of who should have access to what remote users
    ssh = [ juan gleb alastair ci ];

    # the host needs to actually decrypt the secrets it receives on
    # deployment. gleb is there cause someone needs to set secrets
    # to begin with. will be more granular in the future
    service = [ st0x-op host ];
  };

}
