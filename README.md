# invertIndex
invertIndex by mapreduce
Map的实现
	Map接受的value为文件split的内容，输出的KV对为<word:filename，1>，key为单词和所属的文件名，类型为Text。value为数量1，类型为Intwritable。
	用StringTokennizer对输入的文件按照空格分离出单词，每分离出一个单词，就和文件名组合成一个key值，并用1作为value值将KV对交给Combine处理。
Combine的实现
	Combine接受的KV对为Map的输出，输出的KV对为<word,filename:count>。key为单词，类型为Text。value为文件名和单词在这个文件出现的次数，类型为Text。
	在Combine中统计一个单词在一个文件中的出现次数，并将key值设置为单词后输出给reduce处理。
Reduce的实现
	Reduce接受的KV对为Combine的输出，输出的KV对为<word,<filename1:count1>,<filename2:count2>,…,>。key为单词，类型为Text。value为文件名和单词在这个文件出现次数的汇总，类型为Text。
	在Reduce中将接受的value结合为一个Text即可完成。
