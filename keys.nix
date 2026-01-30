rec {
  keys = {
    gleb =
      "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIHepyxN9hvXzbCY/z0amzldy7DXjNdyetnVaQexRgDEX";
    host =
      "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIFVqPJp5MQ77KmOGkFW1/SzWlhfvh0k2W82EMxoFFxpN";
  };

  roles = {
    infra = [ keys.gleb ];
    service = [ keys.gleb keys.host ];
  };
}
