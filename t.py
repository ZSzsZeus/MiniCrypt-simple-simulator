for i in range(1024 ,4096, 1024):
    print(i)

import numpy as np


def cos_sim(vector_a, vector_b):
    """
    计算两个向量之间的余弦相似度
    :param vector_a: 向量 a 
    :param vector_b: 向量 b
    :return: sim
    """
    vector_a = np.mat(vector_a)
    vector_b = np.mat(vector_b)
    num = float(vector_a * vector_b.T)
    denom = np.linalg.norm(vector_a) * np.linalg.norm(vector_b)
    cos = num / denom
    sim = 0.5 + 0.5 * cos
    return sim

print(cos_sim([2000,2000,2000],[1800,1900,2000]))
print(cos_sim([2000,2000,2000],[1900,1900,2000]))
print(cos_sim([2000,2000,2000],[1800,1800,2000]))

s = np.std([1,2,3,4,50], ddof=1)
print(s)