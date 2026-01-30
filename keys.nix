rec {
  keys = {
    gleb =
      "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIHepyxN9hvXzbCY/z0amzldy7DXjNdyetnVaQexRgDEX";
    host =
      "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAILM3C0/VnY1XpfNG+KUtWpNgwGoj9tu41gMkoJQSz1PC";
    ci =
      "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIARWUchVuQvkFm2tzspdC79hhRyYbWzRjs5iimhxewUy";
  };

  roles = {
    infra = [ keys.gleb keys.ci ];
    service = [ keys.gleb keys.host ];
    ssh = [ keys.gleb keys.ci ];
  };
}
