#include "tasksys.h"
#include <algorithm>


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
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
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
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
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

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
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
                return stop ||  running_task;
            });
        if (stop) return;
        lk.unlock();

        std::unique_lock<std::mutex> lk2(task_graph_mutex);
        if(ready_queue.empty()) {
            continue;
        }
        TaskID task_id = ready_queue.front();
        TaskInfo* task_info = task_map[task_id].get();
        lk2.unlock();
        
        int id = task_info->next_task.fetch_add(1);
        if(id >= task_info->total_tasks) {
            continue;
        }
        task_info->runnable->runTask(id, task_info->total_tasks);
        int finished = task_info->finished_task_count.fetch_add(1) + 1;
        if (finished >= task_info->total_tasks) {
            std::unique_lock<std::mutex> lk3(task_graph_mutex);
            ready_queue.pop();
            num_finished_tasks.fetch_add(1);
            
            for (TaskID dep_task : forward_dependencies[task_id]) {
                auto it = reverse_dependencies.find(dep_task);
                if (it != reverse_dependencies.end()) {
                    std::vector<TaskID>& dep_list = it->second;
                    dep_list.erase(std::remove(dep_list.begin(), dep_list.end(), task_id), dep_list.end());
                    if (dep_list.empty()) {
                        ready_queue.push(dep_task);
                        reverse_dependencies.erase(it);
                    }
                }
            }
            if(num_finished_tasks.load() == total_tasks.load()) {
                std::unique_lock<std::mutex> lk4(finish_lock);
                finish_cv.notify_one();  // 通知主线程任务全部完成
                std::unique_lock<std::mutex> lk5(task_lock);
                running_task = false;  // 重置运行状态
            }
            
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

    TaskInfo* task_info = new TaskInfo;
    task_info->runnable = runnable;
    task_info->total_tasks = num_total_tasks;
    task_info->next_task = 0;
    task_info->finished_task_count = 0;

    TaskID task_id = task_id_counter.fetch_add(1);
    std::unique_lock<std::mutex> lk(task_graph_mutex);
    task_map[task_id] = std::shared_ptr<TaskInfo>(task_info);
    total_tasks.fetch_add(1);
    ready_queue.push(task_id);
    lk.unlock();


    {
        std::unique_lock<std::mutex> lk2(task_lock);
        running_task = true;
        task_cv.notify_all();  // 唤醒所有线程开始处理任务
    }

    // 等待线程池完成所有任务
    std::unique_lock<std::mutex> lk3(finish_lock);
    finish_cv.wait(lk3, [this]() {
        return num_finished_tasks.load() == total_tasks.load();
    });


}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    // 1. 创建一个新的 TaskInfo 对象
    TaskInfo* task_info = new TaskInfo;
    task_info->runnable = runnable;
    task_info->total_tasks = num_total_tasks;
    task_info->next_task = 0;
    task_info->finished_task_count = 0;
    TaskID task_id = task_id_counter.fetch_add(1);
    total_tasks.fetch_add(1);
    // 2. 将任务添加到任务图中
    std::unique_lock<std::mutex> lk(task_graph_mutex);
    task_map[task_id] = std::shared_ptr<TaskInfo>(task_info);
    // 3. 如果没有依赖关系，则将任务添加到就绪队列
    if (deps.empty()) {
        ready_queue.push(task_id);
    }
    // 4. 将依赖关系添加到反向依赖关系图中
    reverse_dependencies[task_id] = deps;
    // 5. 对于每个依赖任务，将其添加到正向依赖关系图中
    for (const auto& dep : deps) {
        forward_dependencies[dep].push_back(task_id);
    }
    lk.unlock();

    {
        std::unique_lock<std::mutex> lk2(task_lock);
        running_task = true;
        task_cv.notify_all();  // 唤醒所有线程开始处理任务
    }
    return task_id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    // 等待线程池完成所有任务
    std::unique_lock<std::mutex> lk(finish_lock);
    finish_cv.wait(lk, [this]() {
        return num_finished_tasks.load() == total_tasks.load();
    });
    lk.unlock();

    // 清空任务图
    std::unique_lock<std::mutex> lk2(task_graph_mutex);
    for (auto& task : task_map) {
        task.second.reset();
    }
    task_map.clear();
    reverse_dependencies.clear();
    forward_dependencies.clear();
    num_finished_tasks = 0;
    total_tasks = 0;
    task_id_counter = 0;

    
    return;
}
