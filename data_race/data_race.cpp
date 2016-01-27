#include <thread>
#include <cstdlib>
#include <atomic>
#include "windows_semaphore.hpp"

int main(void)
{
	using semaphore_type = semaphore;

	semaphore_type sema_1{ 1 };
	semaphore_type sema_2{ 1 };
	semaphore_type end_sema{ 2 };

	int x, y, r1, r2;

	std::thread t1
	{
		[&]()
		{
			while (true)
			{
				sema_1.wait();
				while (std::rand() % 8 != 0);
				x = 1;
				//std::atomic_thread_fence(std::memory_order_seq_cst);
				std::atomic_signal_fence(std::memory_order_seq_cst);
				r1 = y;
				end_sema.signal();
			}
		}
	};

	std::thread t2
	{
		[&]()
		{
			while (true)
			{
				sema_2.wait();
				while (std::rand() % 8 != 0);
				y = 1;
				//std::atomic_thread_fence(std::memory_order_seq_cst);
				std::atomic_signal_fence(std::memory_order_seq_cst);
				r2 = x;
				end_sema.signal();
			}
		}
	};

	int detected = 0;
	for (auto iterations = 0; ; ++iterations)
	{
		x = 0; y = 0; r1 = 0; r2 = 0;
		sema_1.signal();
		sema_2.signal();
		end_sema.wait();
		end_sema.wait();

		if (r1 == 0 && r2 == 0)
		{
			detected++;
			printf("%d reorders detected after %d iterations\n", detected, iterations);
		}
	}
	
	return 0;
}