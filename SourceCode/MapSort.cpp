/*********************************
			MAP SORT
**********************************/
#include <iostream>
#include <pthread.h>
#include <time.h>
#include <fstream>
#include <stdlib.h>
#include <list>
#include <algorithm>
using namespace std;

//Maximum limit on the number of threads
#define MAX 50

/************************************
			GLOBALS  	  
*************************************/ 
//Size of the input array
int input_size ;
//Number of intervals in which data will be divided
int L ;
//Number of subsets = Number of threads used for segregation
int S ;
//Pointer to input array
int* array ;
//Pointer to resultant sorted array
int* results ;

//Counts[i][j] to keep track of number of elements
// in subset i , belonging to interval j. 
int Counts[MAX][MAX];
//Values used as intervals
int intervals[MAX] ;

//Shared Barrier
pthread_barrier_t our_barrier;


/************************************
		Sequential Quisk Sort
Used for sorting the subsets formed
after segregating them according to 
intervals				  
*************************************/
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
        /* pi is partitioning index, arr[p] is now
           at right place */
        int pi = partition(arr, low, high);
 
        // Separately sort elements before
        // partition and after partition
        quickSort(arr, low, pi - 1);
        quickSort(arr, pi + 1, high);
    }
}

/****************************************
    	MAPSORT IMPLEMENTATION
*****************************************/

/*****************************************
	Function : calculateStartIndex
 - calculates the initial index for a given
   thread and a given interval where that 
   can begin writing in the results array
 - called during the write-back phase of the
   algorithm
********************************************/  

int calculateStartIndex(int tid, int interval)
{
	int start_index= 0;

	//Counting the number of elements appearing
	//before this interval i.e the starting index
	//of this interval
	for(int j=0 ; j<interval ; j++)
	{
		for(int i=0 ; i<S ; i++)
		{
			start_index += Counts[i][j];
		}
	}

	//Count where in the interval the current thread will 
	//start writing
	for(int i=0 ; i<tid ; i++)
	{
		start_index = start_index + Counts[i][interval];
	}

	return start_index;
}


/*****************************************
		Function : intervalSort
 - function executed by Sorter threads to
 sort the intervals formed till the preceding 
 stage
********************************************/
void *intervalSort(void* id)
{
	int tid = (long) id ;

	int start_index = 0;
	int end_index = 0;

	int interval_length = 0 ;

	//Calculating lenth of the interval to be sorted
	for(int i=0 ; i<S ; i++)
	{
		//Iterating over the interval column of Counts[][]
		interval_length += Counts[i][tid];
	}

	//Start index for this interval = total number of elements that appear
	// in previous intervals
	for(int i=0 ; i<tid ; i++)
	{
		//Calculating the # of elements in i-th interval
		for(int j=0 ; j<S ; j++)
		{
			start_index += Counts[j][i];
		}
	}

	//end_index = start_index + interval_length ;
    quickSort(results, start_index , start_index+interval_length-1);
    
}


/*****************************************
		Function : produceMappings
 - function executed by Mapping threads to
 count the number of elements belonging to
 different intervals, and updating Counts[][]
 accordingly
 - Followed by writeback to results array
********************************************/

void *produceMappings(void* id)
{
	int tid = (long)id ;

	//Attributes of the Subset the current thread will work on
	int start_index=0;
	int end_index ;
	int subset_size = input_size/S ;

	//Calculating start_index and end_index
	for(int i =0 ; i<tid; i++)
	{

		start_index += subset_size ;
	}

	end_index = start_index + subset_size - 1;

	//If last thread, end_index = last element
	//In case array size is not a multiple of subset size
	if(tid == S-1)
		end_index = input_size-1;

	//Array for saving the intervals the subset elements belong to
	int interval_number[end_index-start_index+1];
	int inter= 0;


	//Finding the intervals, the subset elements belong to
	for(int i= start_index ; i<=end_index ; i++)
	{
		if(array[i]<=intervals[0])
		{
			Counts[tid][0]++;
			interval_number[inter]=0 ;
		}
		else if(array[i]>intervals[L-2])
		{
			Counts[tid][L-1]++;
			interval_number[inter]=L-1;
		}
		else
		{
			for(int l=1 ; l<L-1 ; l++)
			{
				if(array[i]>intervals[l-1] && array[i]<=intervals[l])
				{
					Counts[tid][l]++ ;
					interval_number[inter]=l;
				}
			}
		}

		inter++;
	}

	
	//Wait until all thread have counted
	pthread_barrier_wait(&our_barrier);

	//****************************************
	//Write-Back to results[ ] begin here
	//****************************************

	//Array to save the start index for current thread
	//for all the intervals in result[ ]
	//**NOTE : This makes sure no thread overlaps and can
	//         write concurrently
	int final_start_index[L];
	int interval_counts[L] = {0};

	for(int i=0 ; i<L ; i++)
	{
		final_start_index[i] = calculateStartIndex(tid, i);
	}

	int num_of_el = end_index - start_index + 1 ;

	//Write Back
	for(int j=0 ; j<num_of_el ; j++)
	{
		int inter = interval_number[j];
		//Position = start_index for that interval for current thread + 
		//           number of elements already inserted in it
		int pos = final_start_index[inter]+ interval_counts[inter];

		results[pos] = array[start_index+j];

		//One more element of that interval inserted
		interval_counts[interval_number[j]]++ ;

	}

	pthread_exit(NULL);
}


/**********************************************
		Function : mapSort
- main mapSort function responsible for choosing
interval values and spawning threads
********************************************/

void MapSort(int array[])
{
	//To calculate execution time
	clock_t t1,t2 ;
	t1=clock();

	//List to store indices of elements to be used as intervals
	list<int> indices ; 
	
	srand(time(NULL));
	int count = 0 ;

	//Populating indices
	while(count<L)
	{
		int temp =  rand() % input_size ;

		auto it  = std::find(indices.begin(), indices.end(), temp);

		if(it==indices.end())
		{
			indices.push_back(temp);
			intervals[count-1]=array[temp] ;
			count++ ;
		}
	}

	//Sort the interval values
	quickSort(intervals, 0 ,L-2);


	//Initializinf counts to zero
	for(int i=0 ; i<S ; i++)
	{
		for(int j=0 ; j<L ; j++)
		{
			Counts[i][j]=0;
		}
	}

	//Spawning Mapper threads
	pthread_attr_t attr ;
	pthread_t thread_array[S] ;

	pthread_attr_init(&attr);
	void* status ;

	pthread_barrier_init(&our_barrier,NULL,S);	

	for(int i=0 ; i<S ; i++)
	{
		pthread_create(&thread_array[i] , NULL, produceMappings, (void*)i ); 
	}

	for(int j=0 ; j<S ; j++)
	{
		pthread_join(thread_array[j],&status); 
	}


	//Spawning Sorter Threads
	pthread_t threads[L];

	for(int i=0 ; i<L ; i++)
	{
		pthread_create(&threads[i] , NULL, intervalSort, (void*)i ); 
	}

	for(int j=0 ; j<L ; j++)
	{
		pthread_join(threads[j],&status); 
	}

	t2 = clock() - t1;
	double sec =(((double) t2)/CLOCKS_PER_SEC);

	cout<<"\nTime Taken : "<<sec<<endl;
	pthread_attr_destroy(&attr); 

}



int main()
{
	string filename ;

	cout<<"Enter File Name : ";
	cin>>filename;

	cout<<"Input array size : ";
	cin>>input_size ;

	//Allocating Space for input and results array
	array = new int[input_size];
	results = new int[input_size];

	//Reading data from input file
	ifstream  input;
	input.open(filename);

	for(int i =0 ; i<input_size ; i++)
	{
		input>>array[i];
	}

	cout<<"Enter the number of subsets : ";
	cin>>S;

	cout<<"Enter the number of Intervals : ";
	cin>>L;

	MapSort(array);


	//Writing results to output file
	ofstream output ;
	output.open("MapsortOutput.txt", ios::out);

	for(int i=0 ; i<input_size ; i++)
	{
		output<<results[i]<<" ";
	}

	output.close();
	input.close();
	

	return 0; 

}	