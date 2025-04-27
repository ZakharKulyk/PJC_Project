#include <iostream>
#include <vector>
#include <algorithm>
#include <map>
#include <string>
#include <regex>
#include <fmt/base.h>
#include <fmt/ranges.h>
#include <variant>


using namespace std;

using ColumnValue = variant<int, float, string>;  // Define possible types for columns


void printColumnValue(const ColumnValue &value) {
    // Use fmt::print to handle different types in the variant
    visit([](const auto &val) { fmt::print("{} ", val); }, value);
}


std::string toString(const ColumnValue &val) {
    return std::visit([](auto &&arg) -> std::string {
        std::ostringstream oss;
        oss << arg;
        return oss.str();
    }, val);
}

template<typename T>
class RowColumn {
public:
    map<string, vector<ColumnValue>> rowColumn;
};

template<typename T>
class Tables {
public:
    map<string, RowColumn<T>> tables;
    map<string, vector<string>> primaryKeys;
};


class WhereCondition {
public:
    string column;
    string operation;
    string value;

    WhereCondition(string col, string op, string val)
            : column(col), operation(op), value(val) {}
};

class WherePattern {
public:
    vector<WhereCondition> conditions;  // Stores all conditions
    vector<string> logicalOperators;    // Stores logical operators like AND, OR
};


void deleteTable(string tableName, Tables<int> &tables) {
    tables.tables.erase(tableName);
}

namespace DBCommands {
    const string create = "create";
    const string insert = "insert";
    const string select = "select";
    const string where = "where";
}


void toLower(vector<string> &query) {
    for (string &str: query) {
        transform(str.begin(), str.end(), str.begin(), [](unsigned char c) {
            return tolower(c);
        });
    }
}

vector<string> deleteSpaces(string &line) {
    regex rgx("\\s+");

    // Use regex to split the string by one or more spaces
    vector<std::string> words(
            sregex_token_iterator(line.begin(), line.end(), rgx, -1),
            sregex_token_iterator()
    );

    // Return the vector of words
    return words;
}

bool processPrimaryKeysWithCreate(vector<string> query, Tables<int> &tables) {
    vector<vector<string>::iterator> primaryLocationVector;
    for (auto i = query.begin(); i != query.end(); i++) {
        if (*i == "primary") {
            primaryLocationVector.push_back(i);
        }
    }

    if (primaryLocationVector.empty()) {
        fmt::println("{}", "no primary key in table !");
        return false;
    }
    for (auto primaryLocation: primaryLocationVector) {
        auto keyLocation = primaryLocation + 1;

        if (*(keyLocation + 1) == "(") {
            auto tableName = query[1];
            auto nameOfPrimaryKeyColumn = *(keyLocation + 2);

            if (!(tables.tables[tableName].rowColumn.contains(nameOfPrimaryKeyColumn))) {
                fmt::println("no such column exist {}", nameOfPrimaryKeyColumn);
                tables.primaryKeys.erase(tableName);
                return false;
            }

            tables.primaryKeys[tableName].push_back(nameOfPrimaryKeyColumn);
            continue;
        } else {
            auto tableName = query[1];
            auto nameOfPrimaryKeyColumn = *(primaryLocation - 2);
            if (!(tables.tables[tableName].rowColumn.contains(nameOfPrimaryKeyColumn))) {
                fmt::println("no such column exist {}", nameOfPrimaryKeyColumn);
                tables.primaryKeys.clear();
                return false;
            }
            tables.primaryKeys[tableName].push_back(nameOfPrimaryKeyColumn);
        }

    }
    return true;

}


void processCreate(vector<string> query, Tables<int> &tables) {

    string tableName = query[1];

    // Initialize the RowColumn for this table
    RowColumn<int> data;

    // Process the column definitions inside parentheses
    bool insideParentheses = false;
    string currentColumnName;
    string currentColumnType;

    for (int i = 2; i < query.size(); ++i) {
        string word = query[i];
        if (word == "primary") {
            if (query[i + 1] == "key") {
                if (query[i + 2] == "(") {
                    i = i + 4;
                    continue;
                }
                i = i + 1;
                continue;
            }
        }
        if (word == "(") {
            insideParentheses = true;
            continue;
        }
        if (word == ")") {
            insideParentheses = false;
            continue;
        }

        if (insideParentheses) {
            if (currentColumnName.empty()) {
                // It's the column name
                currentColumnName = word;
            } else {
                // It's the column type
                currentColumnType = word;

                // Initialize the vector with the appropriate default value
                if (currentColumnType == "int") {
                    // Store a vector of ColumnValue with a default value (int)
                    data.rowColumn[currentColumnName] = {ColumnValue(0)};  // Correct for vector of ColumnValue
                } else if (currentColumnType == "string") {
                    // Store a vector of ColumnValue with a default value (empty string)
                    data.rowColumn[currentColumnName] = {ColumnValue("")};  // Correct for vector of ColumnValue
                } else if (currentColumnType == "float") {
                    // Store a vector of ColumnValue with a default value (float)
                    data.rowColumn[currentColumnName] = {ColumnValue(0.0f)};  // Correct for vector of ColumnValue
                }

                // Reset for next column definition
                currentColumnName.clear();
                currentColumnType.clear();
            }
        }
    }

    // Add this table to the tables map
    tables.tables[tableName] = data;
    if (!processPrimaryKeysWithCreate(query, tables)) {
        deleteTable(tableName, tables);
    }

}

auto defineNumberOfInsertStatements(vector<string> query) {
    vector<vector<string>> result;
    vector<string>::iterator beginRange;
    vector<string>::iterator endRange;
    int countOfPassedParentheses;

    for (auto i = query.begin(); i != query.end(); ++i) {
        if (*i == DBCommands::insert) {
            int numberOfIterationsDone;
            beginRange = i;
            while (*i != "(") {
                ++numberOfIterationsDone;
                ++i;

            }
            ++countOfPassedParentheses;
            while (*i != ")") {
                ++numberOfIterationsDone;
                ++i;
            }
            ++countOfPassedParentheses;
            if (countOfPassedParentheses == 2) {
                endRange = i + numberOfIterationsDone;
                vector<string> sub = vector<string>(beginRange, endRange);
                result.push_back(sub);
                countOfPassedParentheses = 0;
                numberOfIterationsDone = 0;
            }

        }
    }

    return result;
}

auto processWhereStatement(const vector<string>& query) {
    auto whereLoc = find(query.begin(), query.end(), DBCommands::where);

    // Create an empty WherePattern
    WherePattern wherePattern;

    // Start reading from after the 'WHERE' keyword
    auto conditionStart = whereLoc + 1;

    // Parse conditions and operators
    string currentColumn;
    string currentOp;
    string currentValue;
    string currentOperator = "";  // Initialize as empty, will change to AND/OR

    for (auto it = conditionStart; it != query.end(); ++it) {
        // Handle logical operators (AND/OR) in a case-insensitive manner
        if (*it == "and" || *it == "or") {
            wherePattern.logicalOperators.push_back(*it);  // Store AND/OR
        } else if (currentColumn.empty()) {
            currentColumn = *it;  // First part of the condition (column)
        } else if (currentOp.empty()) {
            currentOp = *it;  // The operator part of the condition (e.g., >, <, =)
        } else {
            currentValue = *it;

            // Here we don't need to strip quotes, as the value is just a string directly
            wherePattern.conditions.push_back(WhereCondition(currentColumn, currentOp, currentValue));

            // Reset for the next condition
            currentColumn.clear();
            currentOp.clear();
            currentValue.clear();
        }
    }

    return wherePattern;
}

void processSelect(const vector<string>& query, Tables<int>& tables) {
    if (query.size() < 2) {
        fmt::println("Invalid SELECT format.");
        return;
    }

    if (find(query.begin(), query.end(), DBCommands::insert) != query.end() ||
        find(query.begin(), query.end(), DBCommands::create) != query.end()) {
        fmt::println("Select query cannot contain keywords for Insert or Create.");
        return;
    }

    bool isSelectingColumns = true;
    vector<string> targetedColumns;
    string tableName;

    for (auto it = query.begin() + 1; it != query.end(); ++it) {
        if (*it == "from") {
            isSelectingColumns = false;
            continue;
        }

        if (isSelectingColumns) {
            targetedColumns.push_back(*it);
        } else {
            tableName = *it;
            break;
        }
    }

    if (tables.tables.count(tableName) == 0) {
        fmt::println("No such table exists: '{}'", tableName);
        return;
    }

    auto& columns = tables.tables[tableName].rowColumn;

    vector<string> actualColumnsToPrint;
    bool isWherePresent = false;
    WherePattern pattern;

    if (std::find(query.begin(), query.end(), DBCommands::where) != query.end()) {
        pattern = processWhereStatement(query);
        isWherePresent = true;
    }

    // Add targeted columns for SELECT
    if (targetedColumns.size() == 1 && targetedColumns[0] == "*") {
        // Select all columns
        for (const auto& pair : columns) {
            actualColumnsToPrint.push_back(pair.first);
        }
    } else {
        for (const auto& target : targetedColumns) {
            if (!columns.contains(target)) {
                fmt::println("No such column '{}' in table '{}'", target, tableName);
                return;
            }
            actualColumnsToPrint.push_back(target);
        }
    }

    // Print header
    for (const auto& colName : actualColumnsToPrint) {
        fmt::print("| {:15} ", colName);
    }
    fmt::print("|\n");

    // Print separator
    for (size_t i = 0; i < actualColumnsToPrint.size(); ++i) {
        fmt::print("|{:-^17}", "");
    }
    fmt::print("|\n");

    // Find number of rows
    int numRows = columns.begin()->second.size();

    // Print each row
    for (int rowIdx = 0; rowIdx < numRows; ++rowIdx) {
        bool conditionPass = true;  // Assume the row is valid until proven otherwise
        bool hasPassedAnyCondition = false;  // For OR logic

        // Evaluate WHERE conditions
        if (isWherePresent) {
            for (size_t i = 0; i < pattern.conditions.size(); ++i) {
                const auto& condition = pattern.conditions[i];
                const auto& columnData = columns.at(condition.column);
                const auto& cell = columnData[rowIdx];

                bool currentConditionPass = false;

                // Check if the value in the column matches the operation
                if (std::holds_alternative<int>(cell)) {
                    int cellValue = std::get<int>(cell);
                    int targetValue = std::stoi(condition.value);

                    if (condition.operation == ">") {
                        currentConditionPass = (cellValue > targetValue);
                    } else if (condition.operation == "<") {
                        currentConditionPass = (cellValue < targetValue);
                    } else if (condition.operation == ">=") {
                        currentConditionPass = (cellValue >= targetValue);
                    } else if (condition.operation == "<=") {
                        currentConditionPass = (cellValue <= targetValue);
                    } else if (condition.operation == "=") {
                        currentConditionPass = (cellValue == targetValue);
                    }
                } else if (std::holds_alternative<std::string>(cell)) {
                    const std::string& cellValue = std::get<std::string>(cell);

                    if (condition.operation == "=") {
                        currentConditionPass = (cellValue == condition.value);
                    } else if (condition.operation == ">") {
                        currentConditionPass = (cellValue > condition.value);
                    } else if (condition.operation == "<") {
                        currentConditionPass = (cellValue < condition.value);
                    } else if (condition.operation == ">=") {
                        currentConditionPass = (cellValue >= condition.value);
                    } else if (condition.operation == "<=") {
                        currentConditionPass = (cellValue <= condition.value);
                    }
                }

                // Apply logical operators:
                if (pattern.logicalOperators.size() > 0 && i > 0) {
                    if (pattern.logicalOperators[i - 1] == "and" && !currentConditionPass) {
                        conditionPass = false;  // All must pass for AND
                        break;  // No need to check further if it's an AND condition and failed
                    }
                    if (pattern.logicalOperators[i - 1] == "or" && currentConditionPass) {
                        hasPassedAnyCondition = true;  // Only one needs to pass for OR
                    }
                } else {
                    // For the first condition, we just check
                    conditionPass = currentConditionPass;
                }
            }
        }

        // Only print the row if the condition passes (AND/OR logic)
        if (conditionPass || hasPassedAnyCondition) {
            for (const auto& colName : actualColumnsToPrint) {
                printColumnValue(columns.at(colName)[rowIdx]);
                fmt::print("{: <5}", ""); // Small gap after value
            }
            fmt::print("\n");
        }
    }
}


void processInsert(const vector<string> &query, Tables<int> &tables) {
    if (query.size() < 7 || query[0] != "insert" || query[1] != "into") {
        fmt::println("Invalid insert statement.");
        return;
    }

    string tableName = query[2];


    if (!tables.tables.contains(tableName)) {
        fmt::println("Table '{}' does not exist.", tableName);
        return;
    }

    RowColumn<int> &table = tables.tables[tableName];

    // Parse column names inside parentheses
    int startIdx = 2;
    while (startIdx < query.size() && query[startIdx] != "(") ++startIdx;
    int endIdx = startIdx + 1;
    while (endIdx < query.size() && query[endIdx] != ")") ++endIdx;


    vector<string> columnNames(query.begin() + startIdx + 1, query.begin() + endIdx);

    int starIdxForValues = endIdx + 1;
    while (starIdxForValues < query.size() && query[starIdxForValues] != "(") ++starIdxForValues;
    int endIdxForValues = starIdxForValues + 1;
    while (endIdxForValues < query.size() && query[endIdxForValues] != ")") ++endIdxForValues;
    // Parse relative column values after key word Values
    vector<string> columnValues(query.begin() + starIdxForValues + 1, query.begin() + endIdxForValues);

    if (columnNames.size() != columnValues.size()) {
        fmt::println("{}", "there is mismatch in desired values to be inserted and predifined columns ");
        return;
    }


    map<string, string> columnsToValue;

    for (int i = 0; i < columnNames.size(); i++) {
        columnsToValue[columnNames[i]] = columnValues[i];
    }

    auto vectorOfPrimaryKeys = tables.primaryKeys[tableName];

    if (!vectorOfPrimaryKeys.empty()) {
        // Build composite key for the new row
        vector<string> newCompositeKey;
        for (const auto &pk: vectorOfPrimaryKeys) {
            newCompositeKey.push_back(columnsToValue[pk]);
        }

        // Check against existing rows
        int numRows = table.rowColumn.begin()->second.size(); // number of rows
        for (int row = 0; row < numRows; ++row) {
            vector<string> existingCompositeKey;
            for (const auto &pk: vectorOfPrimaryKeys) {
                existingCompositeKey.push_back(toString(table.rowColumn[pk][row]));
            }
            if (existingCompositeKey == newCompositeKey) {
                fmt::println("Composite primary key constraint violated! Duplicate entry.");
                return;
            }
        }
    }

    // ---- INSERT VALUES ----
    for (auto &[colName, colValues]: table.rowColumn) {
        if (!columnsToValue.contains(colName)) {
            fmt::println("Column '{}' missing from insert statement.", colName);
            return;
        }
        string valToInsert = columnsToValue[colName];
        ColumnValue typedVal;

        if (holds_alternative<int>(colValues.front())) {
            typedVal = stoi(valToInsert);
        } else if (holds_alternative<float>(colValues.front())) {
            typedVal = stof(valToInsert);
        } else {
            typedVal = valToInsert;
        }

        colValues.push_back(typedVal);
    }

    fmt::println("Inserted into table '{}'", tableName);
}


auto defineNumberOfCreateStatements(vector<string> query) {
    vector<vector<string>> result;
    vector<string>::iterator beginRange;
    vector<string>::iterator endRange;
    bool constructionValid = false;
    bool isCreateStatement = false;

    for (auto i = query.begin(); i != query.end(); i++) {

        if (*i == DBCommands::create) {
            beginRange = i;
            isCreateStatement = true;
        }

        if (*i == "primary" && isCreateStatement) {
            if (*(i + 1) == "key") {
                if (*(i + 2) == "(") {
                    i = i + 4;
                    continue;
                }
                i = i + 1;
                continue;
            }
        }

        if (*i == ")" && isCreateStatement) {
            endRange = i + 1;
            constructionValid = true;
        }

        if (constructionValid) {
            auto temp = vector<string>(beginRange, endRange);
            result.push_back(temp);
            constructionValid = false;
            isCreateStatement = false;
        }

    }

    return result;

}

void processQuery(vector<string> query, Tables<int> &tables) {
    if (query[0] == "exit") {
        return;
    }


    if (query[0] == DBCommands::select) {
        processSelect(query, tables);
        return;
    }

    auto vectorOfCreateQueries = defineNumberOfCreateStatements(query);

    for (const auto &item: vectorOfCreateQueries) {
        processCreate(item, tables);
    }


    auto vectorOfInsertQueries = defineNumberOfInsertStatements(query);

    for (auto item: vectorOfInsertQueries) {
        processInsert(item, tables);
    }

    return;
}




// Define static member outside the class


void startProgram() {
    vector<string> query;
    string line;
    Tables<int> tables;


    cout << "Program started, now you can enter sql commands\n";

    while (true) {
        if (query.size() == 1 && query[0] == "exit") {
            break;
        }
        cout << "Enter your multi-line SQL statement (press Enter on an empty line to finish):\n";
        while (getline(cin, line)) {
            if (line.empty()) break;// stop on empty line
            auto words = deleteSpaces(line);
            for (auto &word: words) {
                query.push_back(word);
            }
        }

        toLower(query);

        processQuery(query, tables);
        query.clear();
        cout << "query was entered\n";


    }
    cout << "programm stopped\n";
}


int main() {
    startProgram();
}
