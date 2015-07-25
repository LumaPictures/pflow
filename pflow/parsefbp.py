import pyparsing
#from pyparsing import ...

# Refs:
# https://github.com/jpaulm/parsefbp
# https://github.com/noflo/fbp#readme

'''
#comment
INPORT=A.IN:G_IN
OUTPORT=C.OUT:G_OUT
'IIP' -> IN A(Component1) OUT -> IN C(Component3)
A() OUT -> IN B(Component2:bar)
A OUT -> IN B()
A OUT -> IN B, C OUT -> IN D(Component4:foo=bar,baz=123)
'''
