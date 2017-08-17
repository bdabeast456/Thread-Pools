import threading
from collections import deque
from collections import Iterable
from math import ceil

# Used to mark the end of a ThreadBay thread's life.
# Internal use only.
class WorkEnd:
	pass

# Used to mark the end of a ThreadBay thread's batch.
# Internal use only.
class BatchEnd:
	def __init__(self):
		pass

# Base class for thread pool architecture.
# Internal use only.
class ThreadBay:
	def __init__(self, mapFunction, numThreads=8):
		self._mapFunction = mapFunction
		if not self._mapFunction:
			self._mapFunction = lambda x: x
		self._numThreads = numThreads

		self._threadPool = list()
		for i in range(self._numThreads):
			self._threadPool.append(threading.Thread(name=str(i), target=self.run))

		self._running = False
		self._stopped = False

	def setMapFunction(self, mapFunction):
		self._mapFunction = mapFunction
		if not self._mapFunction:
			self._mapFunction = lambda x: x

	def notStartedCheck(self):
		if not self._running:
			raise Exception("Threads not started")

	def checkIfIterable(self, arg):
		if not isinstance(arg, Iterable):
			raise Exception("Object not Iterable")

	def start(self):
		if not (self._running or self._stopped):
			self._running = True
			for thread in self._threadPool:
				thread.start()

	def run(self):
		return

	def stop(self):
		self._stopped = True
		self._running = False

# ThreadBay with a work queue (items are placed on a FIFO queue and
# taken off by the spawned threads).
#
# "Public" methods:
#
# __init__()
# Initializes the thread pool
# @param mapFunction, the function to apply to each work item
# @param numThreads, the number of threads to be in the thread pool
# @return object of type ThreadBayQueue
#
# start()
# Starts thread pool's activity -- must be called before others
# @return
#
# queueWork()
# Places a single work item on the queue
# @param work, the item to place on the queue
# @return
#
# queueBatch()
# Places multiple work items on the queue
# @param workBatch, an Iterable object of items to place on the queue
# @return
#
# stop()
# Stops thread pool activity -- do not call other methods after this point
# @return
#
# Methods not listed above should not be called directly.
#
class ThreadBayQueue(ThreadBay):
	def __init__(self, mapFunction, numThreads=8):
		self.queue = deque()
		self.lock = threading.Lock()
		self.cv = threading.Condition(self.lock)

		super().__init__(mapFunction, numThreads)

	def run(self):
		while self._running:
			self.cv.acquire()

			while not (len(self.queue) > 0):
				self.cv.wait()

			work = self.queue.popleft()

			self.cv.notify()
			self.cv.release()

			if type(work) == WorkEnd:
				return

			try:
				self._mapFunction(work)
			except:
				print("Error in Map Function")

	def queueWork(self, work):
		self.notStartedCheck()

		self.cv.acquire()
		self.queue.append(work)
		self.cv.notify()
		self.cv.release()

	def queueBatch(self, workBatch):
		self.checkIfIterable(workBatch)
		self.notStartedCheck()

		self.cv.acquire()

		for work in workBatch:
			self.queue.append(work)

		self.cv.notify()
		self.cv.release()

	def stop(self):
		self.queueBatch([WorkEnd() for _ in range(self._numThreads)])
		super().stop()

# ThreadBay with an array (work is divided into "chunks" and processed).
#
# "Public" methods:
#
# __init__()
# Initializes the thread pool
# @param mapFunction, the function to apply to each work item
# @param resultReturned, whether data should be returned -- queueWork()
#						 will block if True
# @param numThreads, the number of threads to be in the thread pool
# @return object of type ThreadBayArray
#
# start()
# Starts thread pool's activity -- must be called before others
# @return
#
# queueWork()
# Starts data processing on an Iterable object
# @param work, an Iterable object of items to process by chunks
# @return (if resultReturned is True) object of type list with resulting data
#
# stop()
# Stops thread pool activity -- do not call other methods after this point
# @return
#
# Methods not listed above should not be called directly.
#
class ThreadBayArray(ThreadBay):
	def __init__(self, mapFunction, resultReturned=False, numThreads=8):
		self.resultReturned = resultReturned
		self.workList = list()
		self.returnList = list()
		self.workReady = [threading.Semaphore(0) for _ in range(numThreads)]
		self.workDone = threading.Semaphore(0)

		super().__init__(mapFunction, numThreads)

	def run(self):
		while self._running:
			self.workReady[int(threading.current_thread().name)].acquire()

			index = 0
			work = self.getWork(index)

			while type(work) != BatchEnd:
				if type(work) == WorkEnd:
					return

				try:
					tempResult = self._mapFunction(work)

					if self.resultReturned:
						self.recordResult(index, tempResult)
				except:
					self.workDone.release()

					raise Exception("Error in Map Function")

				index += 1
				work = self.getWork(index)

			self.workDone.release()

	# Returns the workList index or None
	def getIndex(self, index):
		threadPlace = int(threading.current_thread().name)
		step = ceil(len(self.workList) / self._numThreads)
		threadStart = threadPlace * step
		threadEnd = (threadPlace + 1) * step

		workLocation = threadStart + index

		if workLocation >= threadStart and \
		   workLocation < threadEnd and \
		   workLocation < len(self.workList):
		   return workLocation
		return BatchEnd()

	def getWork(self, index):
		workLocation = self.getIndex(index)
		if type(workLocation) != BatchEnd:
			return self.workList[workLocation]
		return workLocation

	def recordResult(self, index, tempResult):
		workLocation = self.getIndex(index)
		self.returnList[workLocation] = tempResult

	def queueWork(self, work):
		self.checkIfIterable(work)
		self.notStartedCheck()

		if len(work) == 0:
			return

		self.workList = list(work)
		if self.resultReturned:
			self.returnList = [0 for _ in range(len(self.workList))]

		for sema in self.workReady:
			sema.release()

		if self.resultReturned:
			for i in range(self._numThreads):
				self.workDone.acquire()
			return self.returnList

	def stop(self):
		self.resultReturned = False
		self.queueWork([WorkEnd() for _ in range(self._numThreads)])
		super().stop()
