Gem::Specification.new do |s|
  s.name = "drizzle-ffi"
  s.version = "0.0.4"
  s.date = "2009-08-11"
  s.authors = ["Jake Douglas"]
  s.email = "jakecdouglas@gmail.com"
  s.has_rdoc = false
  s.add_dependency('ffi')
  s.add_dependency('bacon')
  s.summary = "libdrizzle ffi"
  s.homepage = "http://www.github.com/yakischloba/libdrizzle-ruby-ffi"
  s.description = "libdrizzle ffi"
  s.files =
    ["drizzle.gemspec",
    "README.rdoc",
    "Rakefile",
    "lib/drizzle.rb",
    "lib/drizzle/drizzle.rb",
    "tests/basic.rb"]
end
