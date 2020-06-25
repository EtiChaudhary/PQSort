/*******************************
 	PARALLELIZED QUICK SORT
********************************/

#include <iostream>
#include <stdlib.h>
#include <time.h>
#include <pthread.h>
#include <math.h>
#include <list>
#include <algorithm>
#include <atomic>
#include <time.h>
#include <fstream>

using namespace std ;


/************************************
    	BUCKET Data Structure
  - Opertion : Push
  - Used for segregation of input 
  	array elements based on splitters  	  
*************************************/    

//Node of a bucket
struct Node
{
	int data ;
	Node* next = NULL ;

};

class Bucket
{
	public:

	atomic<Node*> head ;  //Top of the bucket
	void push(int x);
};

void Bucket :: push(int x)
{
	Node* newNode = new Node();
	newNode->data = x ;

  while(1)
  {

	Node* oldHead = head.load(std::memory_order_seq_cst);
	newNode->next = oldHead ;

	//Try CASing the top of bucket until successful
	if(head.compare_exchange_strong(oldHead,newNode, std::memory_order_seq_cst))
		return;
	
   }

}

/************************************
			GLOBALS  	  
*************************************/    

//Points to an array of buckets based on
// number of threads
Bucket* buckets ;

//Barrier used after initial segregation
//into buckets
pthread_barrier_t our_barrier;

//Points to an array of atomic int
//to store the number of elements in each bucket
atomic<int>* counts ;

int* array ; 
int input_size ;
int* initial_splitters ;
int* intermediate_splitters;
int thread_count ;

/*************************************
   		SEQUENTIAL QUICKSORT
**************************************/   

void swap(int* a, int* b)
{
    int t = *a;
    *a = *b;
    *b = t;
}

int partition (int arr[], int low, int high)
{
    int pivot = arr[high];    // pivot
    int i = (low - 1);  // Index of smaller element
 
    for (int j = low; j <= high- 1; j++)
    {
        if (arr[j] <= pivot)
        {
            i++; 
            swap(&arr[i], &arr[j]);
        }
    }

    swap(&arr[i + 1], &arr[high]);
    return (i + 1);
}
 
void quickSort(int arr[], int low, int high)
{
    if (low < high)
    {
        // pi = partitioning index
        int pi = partition(arr, low, high);
 
        // Separately sort elements before
        // partition and after partition
        quickSort(arr, low, pi - 1);
        quickSort(arr, pi + 1, high);
    }
}

/****************************************
    	PARALLELIZED QUICK SORT
*****************************************/    

/*****************************************
    	Function : formBuckets
 - takes thread ID as input   
 - each thread finds the part of array 
   it will work on
 - traverses it's part, and pushes the
   elements into the corresponding buckets
********************************************/        
void formBuckets(int id)
{
	int bucket_id ;
	int from = 0;    //the initial index of the part of array current thread will work on
	int till=0 ;     //the final index of the part of array current thread will work on

	//If last thread
	if(id==thread_count-1)
	{
		from = initial_splitters[thread_count-2]+1 ;
		till = input_size-1 ;
	}

	else
	{
		till = initial_splitters[id];

		//If not the first thread
		if(id!=0)
		{
			//Start from element next to preceding part of array
			from = initial_splitters[id-1]+1;
		}
	}

	//Traverse its portion, and push elements into buckets based
	//on intermediate splitters
	for(int i=from ; i<=till ; i++)
	{
		bucket_id = 0 ;  //the bucket the element will be pushed into
		int j=1 ;

		//If element's value less than the first splitter, bucket_id =1
		if(array[i]<=intermediate_splitters[0])
			bucket_id = 1 ;

		//If element value more than the highest splitter, bucket_id = last bucket
		else if(array[i]>intermediate_splitters[thread_count-2])
		{
			bucket_id = thread_count;
		}

		else
		{
			while(j<thread_count)
			{
				if(array[i]> intermediate_splitters[j-1] && array[i]<=intermediate_splitters[j])
				{
					bucket_id = j+1 ;
					break ;
				}
				else
					j++ ;
			}
		}

		//Increment count of that bucket and push the element into it
		counts[bucket_id-1]++ ;
		buckets[bucket_id-1].push(array[i]);

	}

}


/*******************************************
     Function: workForThreads

- calls segregate() function
- all threads wait at the barrier until
  segregation phase is complete
- threads write back to the array from
  their corresponding buckets
- & sort (usin sequential quicksort)
********************************************/

void *workForThreads(void  *id)
{

	int tid = (long)id ;

	formBuckets(tid);   //divide into buckets

	pthread_barrier_wait(&our_barrier);   //wait until everyone arrives

	//Calculating the final partition to be sorted by the thread

	int start=0 ;   //initial intex of final partition
	int end=0;		//ending index of final partition

	//If first thread
	if(id == 0)
	{
		start = 0;
		end = counts[0]-1 ;
	}

	else
	{
		//Counting elements in preceding buckets
		for(int i=0 ; i<tid ; i++)
		{
			start = start + counts[i];
		}

		//All array elements pushed into the previous buckets
		if(start>=input_size)
			start = input_size-1;

		end = start + counts[tid]-1 ;
	}

	//If the bucket for this thread not empty, write back its contents
	//into its final partition
	if(counts[tid]>0)
	{
		Node* bb = buckets[tid].head ;

		for(int wb = start ; wb<= end ; wb++)
		{
			array[wb] = bb->data ;
			bb = bb->next ;
		}

		//Sort final partition
		quickSort(array, start, end);
	}

}


/*******************************************
     Function: Parallel_Quicksort()

- function to perform pre-processing before
  spawning threads
- calculates and saves initial splitters
- chooses s random numbers and further selects
  intermediate splitters

********************************************/
void Parallel_Quicksort(int array[], int first, int last, int i)
{
	int size ;
	int num_of_threads ;
	int num_of_splitters ;

	srand(time(NULL));

	if(first<last)
	{
		size = last+1 ;
		num_of_threads = pow(2, i-1);
		num_of_splitters = pow(2,i-1)-1 ;

		thread_count = num_of_threads ;

		initial_splitters = new int[num_of_splitters] ;

		initial_splitters[0] = size/num_of_threads ;
		for(int i=1 ; i<num_of_splitters ; i++)
		{
			initial_splitters[i] = initial_splitters[i-1]+ ceil(size/num_of_threads);
		}


		//Helper array from which intermediate splitters are chosen
		int s = 100 ;
		int helpers[s] = {-1} ;


		std::list<int> indices ;
		int count = 1 ;


		//Choose elements at random indices, and insert into list

		while(count<=s)
		{
			int temp =  rand() % size ;

			auto it  = std::find(indices.begin(), indices.end(), temp);

			if(it==indices.end())
			{
				indices.push_back(temp);
				helpers[count-1]=array[temp] ;
				count++ ;
			}
		}

		//Sort helpers array
		quickSort(helpers, 0 , s-1);

		//Choose evenly spaced intermediate splitters from helpers array
		int step_size = s/num_of_splitters ;
		intermediate_splitters = new int[num_of_splitters];
		int j=0 ;

		for(int i=0 ; i<s ; i=i+step_size+1)
		{
			if(j<num_of_splitters)
			{
				intermediate_splitters[j]=helpers[i];
				j++;
			}
		}

	}

}


//Helper Function to write array to output file
void printArray(int arr[], int size)
{
	ofstream output ;
	output.open("ParallelQSort.txt", ios::out);

    int i;
    for (i=0; i < size; i++)
        output<<arr[i]<<" ";
}

/*******************************************
     Function: workForThreads

- used for timing both sequential and parallel
  sorts
- spawns threads
- read input from file, writes output to file

********************************************/
int main()
{
	//2 copies of input array for each implementation
	int size ;
	int* s_array ;

	//Variables to time parallel quicksort
	clock_t pt1 , pt2 ;
	double psec ;

	//Variables to time sequential quicksort
	clock_t st1 , st2 ;
	double ssec ;

	string filename;
	cout<<"Enter Input File Name : ";
	cin>>filename;
	
	cout<<"Input array size : ";
	cin>>size ;

	//Globals Initialization
	input_size = size;
	array =new int[size];
	

	s_array = new int[size];

	//Taking input from text file
	ifstream  input;
	input.open(filename);
	for(int i =0 ; i<size ; i++)
	{
		input>>array[i];
		s_array[i] = array[i];
	}


	//Input parameter i decides the number of threads to spawn
	// and the number of splitting that take place  = 2^(i-1)
	int i ;
	cout<<"Enter parameter i : "; 
	cin>>i ;

	thread_count = pow(2,i-1);

	buckets =new Bucket[thread_count];
	counts = new atomic<int>[thread_count];

	for(int i=0 ; i<thread_count ; i++)
	{
		counts[i]=0;
	}

	pt1 = clock();
	Parallel_Quicksort(array, 0, size-1, i);

	pthread_attr_t attr ;
	pthread_t thread_array[thread_count] ;

	pthread_attr_init(&attr);
	void* status ;

	//Initializing thread barrier
	pthread_barrier_init(&our_barrier,NULL,thread_count);	

	//Spawning and Joining threads
	for(int i=0 ; i<thread_count ; i++)
	{
		pthread_create(&thread_array[i] , NULL, workForThreads, (void*)(i) ); 
	}

	for(int j=0 ; j<thread_count ; j++)
	{
		pthread_join(thread_array[j],&status); 
	}


	pt2 = clock() - pt1;
	psec = (((double) pt2)/CLOCKS_PER_SEC); 
	
	pthread_attr_destroy(&attr); 

	//Calling SEQUENTIAL quicksort
	st1 = clock();
	quickSort(s_array, 0 ,size-1);
	st2 = clock() - st1 ;
	ssec = (((double) st2)/CLOCKS_PER_SEC);
	cout<<endl<<endl;


	//Writing Sorted output to file
 	printArray(array, size);

	cout<<"\nRunning Time : \n";
	cout<<fixed;
	cout<<"Parallel : "<<psec<<endl ;
	cout<<"\nSequential : "<<ssec<<endl;

	return 0;
	
}

