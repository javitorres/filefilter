import logging as log
class CompiledCodeCache:

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(CompiledCodeCache, cls).__new__(cls)
            cls._instance._cache = {}
            format = "%(asctime)s %(filename)s:%(lineno)d - %(message)s "
            log.basicConfig(format=format, level=log.INFO, datefmt="%H:%M:%S")
        return cls._instance

    def get_compiled_code(self, id, code):

        compiled_code = self._cache.get(id, None)
        if compiled_code:
            return compiled_code
        else:
            log.info("Code NOT found in cache")
            try:
                code_object = compile(code, 'sumstring', 'exec')
                self._cache[id] = code_object
                return code_object
            except Exception as e:
                log.error(f"\t\tError compiling python code: {e}")
                return None




# Uso de la clase
'''
cache = CompiledCodeCache()
cache.add_compiled_code("test1", "a = 5\nb=10\nprint('Sum:', a+b)")
compiled_code = cache.get_compiled_code("test1")
if compiled_code:
    exec(compiled_code)
'''