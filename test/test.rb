#! /usr/bin/ruby

require 'kyotocabinet'
require 'rbconfig'

confs = [
         [ ":", 10000 ],
         [ "*", 10000 ],
         [ "%", 10000 ],
         [ "casket.kch", 10000 ],
         [ "casket.kct", 10000 ],
         [ "casket.kcd", 1000 ],
         [ "casket.kcf", 10000 ],
        ]
formats = [
            "kctest.rb order '%s' '%d'",
            "kctest.rb order -rnd '%s' '%d'",
            "kctest.rb order -etc '%s' '%d'",
            "kctest.rb order -rnd -etc '%s' '%d'",
            "kctest.rb order -th 4 '%s' '%d'",
            "kctest.rb order -th 4 -rnd '%s' '%d'",
            "kctest.rb order -th 4 -etc '%s' '%d'",
            "kctest.rb order -th 4 -rnd -etc '%s' '%d'",
            "kctest.rb order -cc -th 4 -rnd -etc '%s' '%d'",
            "kctest.rb wicked '%s' '%d'",
            "kctest.rb wicked -it 4 '%s' '%d'",
            "kctest.rb wicked -th 4 '%s' '%d'",
            "kctest.rb wicked -th 4 -it 4 '%s' '%d'",
            "kctest.rb wicked -cc -th 4 -it 4 '%s' '%d'",
            "kctest.rb misc '%s'",
           ]

system("rm -rf casket*")
rubycmd = Config::CONFIG["bindir"] + "/" + RbConfig::CONFIG['ruby_install_name']
all = confs.size * formats.size
cnt = 0
oknum = 0
confs.each do |conf|
  path = conf[0]
  rnum = conf[1]
  formats.each do |format|
    cnt += 1
    command = sprintf(format, path, rnum)
    printf("%03d/%03d: %s: ", cnt, all, command)
    rv = system("#{rubycmd} -I. #{command} >/dev/null")
    if rv
      oknum += 1
      printf("ok\n")
    else
      printf("failed\n")
    end
  end
end
system("rm -rf casket*")
if oknum == cnt
  printf("%d tests were all ok\n", cnt)
else
  printf("%d/%d tests failed\n", cnt - oknum, cnt)
end
