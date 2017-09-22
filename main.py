import pymorphy2
import re
import pandas as pd

morph = pymorphy2.MorphAnalyzer()

wordPattern = re.compile("((?:[а-яА-Я]+[-]?)*[а-яА-Я]+)")
text_file = open('vsadnik-bez-golovi.txt')
text = text_file.read()
text[:100]
words_list = wordPattern.findall(text)

words_list = list(map(lambda x: x.lower(), words_list))
words_list[:10]

def get_normal(word):
    return morph.parse(word)[0].normal_form

lemms_list = list(map(get_normal, words_list))
lemms_list[:10]

from collections import Counter

frequence = dict(Counter(lemms_list))

{i : frequence[i] for i in list(frequence.keys())[:10]}
freq_df = pd.DataFrame({
    "word": list(frequence.keys()),
    "frequency" : list(frequence.values())
    })
freq_df.sort_values("frequency", ascending=False).head(50)

