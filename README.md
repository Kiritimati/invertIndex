# invertIndex
invertIndex by mapreduce  
用mapreduce实现的倒排索引。  
map输出<filename:word,1>  
combine输出<word,filename:count>  
reduce输出<word,filename1:count1;filename2:count2;....>
