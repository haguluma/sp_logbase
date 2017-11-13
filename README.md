# sp_logbase
Sparkログベース異常発見の研究用リポジトリ

# hou to use
1. getlog.shの内容を各自の環境で書き直す　
2. pythonプログラムに必要なライブラリ群を落とす
3. ```$ bash getlog.sh```でクラスタからログを集める
4. ```$ python3 parse(EXE/GC).py <SPARKアプリケーションID>```で実行、グラフ描画
