import jieba
cut = jieba.cut

from bottle import route,run

def token(sentence):
    seg_list = list(cut(sentence))
    return " ".join(seg_list)

@route('/token/:sentence')
def index(sentence):
    result = token(sentence)
    return "{\"ret\":0, \"msg\":\"OK\", \"terms\":\"%s\"}" % result

if __name__ == "__main__":
    run(host="localhost",port=8282)
