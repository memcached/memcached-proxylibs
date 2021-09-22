# Lua libraries for memcached proxy

Memcached's builtin proxy comes with very few algorithms for hashing and key
distribution. This repository contains compat libs for older or customized
algorithms for backwards compatibility or migration purposes.

As of now this is pre-release and there are no proper makefiles. If you are
an early adopter you will have to look at the example build shell script and
make a few changes in order to compile the libraries.
