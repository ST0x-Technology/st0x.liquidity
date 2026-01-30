rec {
  keys = {
    gleb =
      "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIHepyxN9hvXzbCY/z0amzldy7DXjNdyetnVaQexRgDEX";
    host =
      "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAICHL9ptsIpEU7eB/o4c3yooWw2bnkd1BaZSJ14ZxDblN";
  };

  roles = {
    infra = [ keys.gleb ];
    service = [ keys.gleb keys.host ];
  };
}
