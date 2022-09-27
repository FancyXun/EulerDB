import numpy as np
import base64


a = np.random.random((64))
b = a.tobytes()
c = np.frombuffer(b, dtype=np.float64)
d = base64.b64encode(b).decode("utf-8")

print(a)
print(b)
print(c)
print(len(d))

# e = np.random.random((100, 128))
# print(e[0].shape)