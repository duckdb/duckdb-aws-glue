#include "core/helpers.hpp"

#include "duckdb/common/types/value.hpp"
#include "duckdb/parallel/task_executor.hpp"

#include "utf8proc_wrapper.hpp"

namespace duckdb {

bool TryParseInteger(const string &value, int64_t min, int64_t max, int64_t &result) {
	if (value.empty()) {
		return false;
	}
	auto cast = Value(value).DefaultTryCastAs(LogicalType::BIGINT);
	if (!cast) {
		return false;
	}
	result = cast->GetValue<int64_t>();
	return result >= min && result <= max;
}

bool TryFirstCharacter(const string &value, string &result) {
	if (value.empty() || !Utf8Proc::IsValid(value.c_str(), value.size())) {
		return false;
	}
	int length;
	Utf8Proc::UTF8ToCodepoint(value.c_str(), length, value.size());
	result = value.substr(0, NumericCast<idx_t>(length));
	return true;
}

namespace {

class IndexTask : public BaseExecutorTask {
public:
	IndexTask(TaskExecutor &executor, const std::function<void(idx_t)> &task, idx_t index)
	    : BaseExecutorTask(executor), task(task), index(index) {
	}

	void ExecuteTask() override {
		task(index);
	}
	string TaskType() const override {
		return "RunInParallel";
	}

private:
	const std::function<void(idx_t)> &task;
	idx_t index;
};

} // namespace

void RunInParallel(ClientContext &context, idx_t count, const std::function<void(idx_t)> &task) {
	if (count == 0) {
		return;
	}
	TaskExecutor executor(context);
	vector<unique_ptr<BaseExecutorTask>> tasks;
	for (idx_t i = 0; i < count; i++) {
		tasks.push_back(make_uniq<IndexTask>(executor, task, i));
	}
	executor.ScheduleTasks(std::move(tasks));
	executor.WorkOnTasks();
}

} // namespace duckdb
