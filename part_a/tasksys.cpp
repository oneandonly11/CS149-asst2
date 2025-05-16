#include "tasksys.h"


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //

    thread_pool = new std::thread[num_threads];
    this->num_threads = num_threads;
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {
    delete[] thread_pool;
}

void TaskSystemParallelSpawn::runThread(IRunnable* runnable, int* p_id, std::mutex* lock, int num_total_tasks) {
    int id;
    while (true) {
        lock->lock();
        if (*p_id >= num_total_tasks) {
            lock->unlock();
            return;
        }
        id = *p_id;
        *p_id += 1;
        lock->unlock();
        runnable->runTask(id, num_total_tasks);
    }
}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {

    
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    std::mutex* lock = new std::mutex();
    int* p_id = new int(0);

    for (int i = 0; i < num_threads; i++) {
        thread_pool[i] = std::thread(&TaskSystemParallelSpawn::runThread, this, runnable, p_id, lock, num_total_tasks);
    }

    for (int i = 0; i < num_threads; i++) {
        thread_pool[i].join();
    }

    delete lock;
    delete p_id;

    
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

void TaskSystemParallelThreadPoolSpinning::runThread(TaskState* taskstate) {
    int id;
    while (true) {
        std::unique_lock<std::mutex> lk(*lock);
        if(taskstate->done) {
            lk.unlock();
            return;
        }
        if(taskstate->next_task_id < 0) {
            lk.unlock();
            continue;
        }
        if (taskstate->next_task_id >= taskstate->num_total_tasks) {
            lk.unlock();
            continue;
        }
        id = taskstate->next_task_id;
        taskstate->next_task_id += 1;
        lk.unlock();
        taskstate->runnable->runTask(id, taskstate->num_total_tasks);
        std::unique_lock<std::mutex> lk2(*lock);
        taskstate->num_finished_tasks += 1;
        if (taskstate->num_finished_tasks >= taskstate->num_total_tasks) {
            taskstate->next_task_id = -1;
            taskstate->num_finished_tasks = 0;
            lk2.unlock();
            cv->notify_all();
        } else {
            lk2.unlock();
        }
    }
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //

    this->num_threads = num_threads;
    thread_pool = new std::thread[num_threads];
    taskstate = new TaskState;
    taskstate->next_task_id = -1;
    taskstate->num_total_tasks = -1;
    taskstate->num_finished_tasks = -1;
    taskstate->runnable = nullptr;
    taskstate->done = false;
    lock = new std::mutex();
    cv = new std::condition_variable();
    for (int i = 0; i < num_threads; i++) {
        thread_pool[i] = std::thread(&TaskSystemParallelThreadPoolSpinning::runThread, this, taskstate);
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    std::unique_lock<std::mutex> lk(*lock);
    taskstate->done = true;
    taskstate->next_task_id = 0;
    lk.unlock();
    cv->notify_all();
    for (int i = 0; i < num_threads; i++) {
        thread_pool[i].join();
    }

    delete[] thread_pool;
    delete taskstate;
    delete lock;
    delete cv;
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    std::unique_lock<std::mutex> lk(*lock);
    taskstate->runnable = runnable;
    taskstate->num_total_tasks = num_total_tasks;
    taskstate->num_finished_tasks = 0;
    taskstate->next_task_id = 0;
    cv->wait(lk, [this] { return taskstate->next_task_id < 0; });
    lk.unlock();
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

void TaskSystemParallelThreadPoolSleeping::runThread() {
    while (true) {
        std::unique_lock<std::mutex> lk(task_lock);
            task_cv.wait(lk, [this]() {
                return stop || next_task_id < total_tasks;
            });
        if (stop) return;
        lk.unlock();

        int id = next_task_id.fetch_add(1);
        if (id >= total_tasks) {
            continue;
        }
        

        runnable->runTask(id, total_tasks);

        int finished = num_finished_tasks.fetch_add(1) + 1;
        if (finished >= total_tasks) {
            std::unique_lock<std::mutex> lk2(finish_lock);
            finish_cv.notify_one();  // 通知主线程任务全部完成
        }
    }
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads)
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //

    : ITaskSystem(num_threads), num_threads(num_threads), stop(false) {
    for (int i = 0; i < num_threads; ++i) {
        thread_pool.emplace_back(&TaskSystemParallelThreadPoolSleeping::runThread, this);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //

    {
        std::unique_lock<std::mutex> lk(task_lock);
        stop = true;
        task_cv.notify_all();  // 唤醒所有线程让它们退出
    }
    // 等待所有线程退出（此实现中线程会在 next_task_id >= total_tasks 时自动退出）
    for (auto& thread : thread_pool) {
        if (thread.joinable()) {
            thread.join();
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    this->runnable = runnable;
    this->total_tasks = num_total_tasks;

    next_task_id = 0;
    num_finished_tasks = 0;

    {
        std::unique_lock<std::mutex> lk(task_lock);
        task_cv.notify_all();  // 唤醒所有线程开始处理任务
    }

    // 等待线程池完成所有任务
    std::unique_lock<std::mutex> lk(finish_lock);
    finish_cv.wait(lk, [this]() {
        return num_finished_tasks.load() >= total_tasks;
    });
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
