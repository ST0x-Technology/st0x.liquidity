rec {
  keys = {
    gleb =
      "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIHepyxN9hvXzbCY/z0amzldy7DXjNdyetnVaQexRgDEX";
    host =
      "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAILM3C0/VnY1XpfNG+KUtWpNgwGoj9tu41gMkoJQSz1PC";
  };

  roles = {
    infra = [ keys.gleb ];
    service = [ keys.gleb keys.host ];
  };
}
