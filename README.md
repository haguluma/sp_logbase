# sp_logbase
Sparkログベース異常発見の研究用リポジトリ

# hou to use
1. getlog.shの内容を各自の環境で書き直す　
2. pythonプログラムに必要なライブラリ群を落とす
3. ```$ bash getlog.sh```でクラスタからログを集める
4. ```$ python3 parse(EXE/GC).py <SPARKアプリケーションID>```で実行、グラフ描画  
### 実行例
```
$ python3 parseGC.py app-20171110175906-0028
$ python3 parseEXE.py app-20171110175906-0028
```
