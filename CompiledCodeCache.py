class CompiledCodeCache:

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(CompiledCodeCache, cls).__new__(cls)
            cls._instance._cache = {}
        return cls._instance

    def get_compiled_code(self, id, code):

        compiled_code = self._cache.get(id, None)
        if compiled_code:
            #print("Returning compiled code")
            return compiled_code
        else:
            #print("Code NOT found in cache")
            code_object = compile(code, 'sumstring', 'exec')
            self._cache[id] = code_object
            return code_object



# Uso de la clase
'''
cache = CompiledCodeCache()
cache.add_compiled_code("test1", "a = 5\nb=10\nprint('Sum:', a+b)")
compiled_code = cache.get_compiled_code("test1")
if compiled_code:
    exec(compiled_code)
'''