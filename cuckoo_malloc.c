#include <stdint.h>
#include <malloc.h>

#include "cuckoo_malloc.h"

void* cuckoo_malloc(size_t size)
{
#if defined(CUCKOO_ENABLE_HUGEPAGE) && defined(__linux__)
	if (size % HUGEPAGE_SIZE != 0) {
		size = (size / HUGEPAGE_SIZE + 1) * HUGEPAGE_SIZE;
	}

	int32_t shmid = shmget(IPC_PRIVATE, size, /*IPC_CREAT |*/ SHM_HUGETLB | SHM_R | SHM_W);
	if (shmid == -1) {
		perror("shmget failed");
		exit(EXIT_FAILURE);
	}

	void *p = shmat(shmid, NULL, 0);
	if (p == (void *)-1) {
		perror("Shared memory attach failed");
		exit(EXIT_FAILURE);
	}

	if (shmctl(shmid, IPC_RMID, NULL) == -1) {
		perror("shmctl failed");
	}

	return p;
#else

	void *p = malloc(size);

	return p;
#endif
}

void cuckoo_free(void *p)
{
#if defined(CUCKOO_ENABLE_HUGEPAGE) && defined(__linux__)
	if (shmdt(p)) {
		perror("");
		assert(0);
	}
#else
	free(p);
#endif
}

