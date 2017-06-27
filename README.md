# recommend-system


# 使用方法：
只要数据库名和各表名没有改动，只要将三个文件中main函数中定义的字典user的值改成当前数据库的用户名和密码即可。

step 1：
运行SetData.py, 根据当前数据库生成推荐时要用的u-i矩阵，并储存在表students_courses和表students_tutors中。

step 2：
分别运行Recommend_courses.py和Recommend_tutors.py, 分别针对每个用户个性化推荐课程和老师，并将结果分别存储在students_courses_recommend_result表和students_tutors_recommend_result表中。
