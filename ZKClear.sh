#清理zkServer生成的日志,缓存文件
#注意:清理后zkServer中的数据将全部丢失,即回到初始状态

cd src/main/resources
rm -rf z1/data/version-2/
rm -rf z2/data/version-2/
rm -rf z3/data/version-2/
rm -rf z4/data/version-2/
cd ../../..;
cd target/classes
rm -rf z1/data/version-2/
rm -rf z2/data/version-2/
rm -rf z3/data/version-2/
rm -rf z4/data/version-2/

echo "清理完成"




