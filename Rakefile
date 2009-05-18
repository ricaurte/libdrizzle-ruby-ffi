require 'rake' unless defined?(Rake)

task :default => [:test]

task :test do
  require 'rubygems'
  require 'lib/drizzle'
  require 'bacon'
  Bacon.summary_on_exit
  load "tests/basic.rb"
end
