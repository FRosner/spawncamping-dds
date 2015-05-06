# pip install jsbeautifier
import jsbeautifier
import sys

if len(sys.argv) < 2:
    print "Please specify the js file to beautify as a command line argument!"
    sys.exit(1)

fileName = sys.argv[1]
print "Beautifying " + fileName

opts = jsbeautifier.default_options()
opts.indent_size = 2
opts.break_chained_methods = True
opts.wrap_line_length = 120
opts.end_with_newline = True

beautified = jsbeautifier.beautify_file(fileName, opts)
f = open(fileName, 'w')
f.write(beautified)
f.close()
