# PQSort
Implementing two versions of Parallel Quicksort

## Intructions to run the programs

- Use the cd command to navigate to current working directory

- To compile the program, execute the following command :
```bash
 g++ -pthread -std=c++11 -w NameOfTheFile.cpp
```

- To run the compiled program, execute the following instruction in terminal
```bash
./a.out
```
- Input Data files should be present in the current working directory
-Parameters :
	- Size between 10^3 and 10^6
	-File name from input data set provided

	-For ParallelQuickSort.cpp
		- Parameter i = 2

	- For MapSort.cpp
		- Number of Subsets = 2
		- Number of intervals = 2 

## References
- [Performance assessment of multithreaded quicksort algorithm on simultaneous multithreaded architecture](https://link.springer.com/article/10.1007/s11227-013-0910-2)
- [Parallelizing Fundamental Algorithms such as Sorting on Multi-core Processors for EDA Acceleration](https://ieeexplore.ieee.org/document/4796485)