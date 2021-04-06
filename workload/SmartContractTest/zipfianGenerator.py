import numpy
import collections
import hashlib

def randZipf(n, alpha, numSamples):
    # Calculate Zeta values from 1 to n:
    tmp = numpy.power(numpy.arange(1, n + 1), -alpha)
    zeta = numpy.r_[0.0, numpy.cumsum(tmp)]
    # Store the translation map:
    distMap = [x / zeta[-1] for x in zeta]
    # Generate an array of uniform 0-1 pseudo-random values:
    u = numpy.random.random(numSamples)
    # bisect them with distMap
    v = numpy.searchsorted(distMap, u)
    samples = [t - 1 for t in v]
    return samples


'''sam = randZipf(1000000, 0.9, 2000)


md5 = hashlib.md5()
data = '1'
md5.update(data.encode('utf-8'))
print(md5.hexdigest())


print(sam)
b = collections.Counter(sam)
a = b.most_common(100)
for c in a:
    print(c)'''
