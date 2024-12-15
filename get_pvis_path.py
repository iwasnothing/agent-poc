import pyvis
import os

pyvis_path = os.path.dirname(pyvis.__file__)
utils_js_path = os.path.join(pyvis_path, 'lib', 'bindings', 'utils.js')
print(utils_js_path)
