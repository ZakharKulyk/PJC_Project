#include <iostream>
#include <vector>
#include <algorithm>
#include <map>
#include <string>
#include <regex>
#include <fmt/base.h>
#include <fmt/ranges.h>
#include <variant>
#include <fstream>
#include <filesystem>


/* This project emulates a simplified Structured Query Language (SQL) engine.
 *
 * -- Data Storage:
 *
 *     * The database engine uses the classes `Tables` and `RowColumn`, combined with `std::map`, for in-memory storage.
 *       The core idea is to represent data in the form of:
 *
 *         map< tableName, map< string, vector<ReferencedType> > >
 *
 *       Here, the inner map represents the table schema, where each key is a column name and the value is a vector
 *       holding the data for that column.
 *
 * -- Features:
 *
 *     * Table creation
 *     * Adding columns
 *     * Dropping columns
 *     * Inserting data
 *     * SELECT statements
 *     * SELECT with multiple WHERE conditions and logical operators (AND, OR)
 *     * UPDATE statements
 *     * UPDATE with multiple WHERE conditions and logical operators
 *     * Adding primary keys
 *     * Adding foreign keys
 *     * Reading SQL commands from a file
 *     * Saving the database state to a file
 *
 * -- Example Queries:
 *
 *     create person (
 *         id int primary key
 *         name string
 *     )
 *
 *     create grade (
 *         mark int primary key
 *     )
 *
 *     create persongrade (
 *         personid int
 *         markid int
 *         primary key ( personid )
 *         primary key ( markid )
 *     )
 *
 *     alter table persongrade foreign key ( personid ) references person ( id )
 *     alter table persongrade foreign key ( markid ) references grade ( mark )
 *
 *     insert into person ( id name ) values ( 1 zakhar )
 *     insert into person ( id name ) values ( 2 roman )
 *     insert into person ( id name ) values ( 3 julia )
 *     insert into person ( id name ) values ( 4 Tom )
 *     insert into person ( id name ) values ( 5 John )
 *     insert into person ( id name ) values ( 6 Peter )
 *
 *     insert into grade ( mark ) values ( 1 )
 *     insert into grade ( mark ) values ( 2 )
 *     insert into grade ( mark ) values ( 3 )
 *     insert into grade ( mark ) values ( 4 )
 *     insert into grade ( mark ) values ( 5 )
 *
 *     insert into persongrade ( personid markid ) values ( 1 1 )
 *     insert into persongrade ( personid markid ) values ( 2 3 )
 *     insert into persongrade ( personid markid ) values ( 3 2 )
 *     insert into persongrade ( personid markid ) values ( 4 5 )
 *     insert into persongrade ( personid markid ) values ( 5 3 )
 *     insert into persongrade ( personid markid ) values ( 6 1 )
 *
 * -- Notes:
 *
 *     * Primary keys can be defined directly in the `create` statement. Multiple `create` statements can be combined
 *       and executed as a single query.
 *
 *     * Different types of `create`, `insert`, and `alter` statements can also be written in a single query.
 *
 *     * The `alter` statement allows:
 *         - Adding foreign keys
 *         - Adding columns
 *         - Dropping columns
 *
 *         Example:
 *             alter table person add email string
 *             alter table person drop email
 *
 *     * The `insert` statement uses the first pair of parentheses to specify the target columns and the second
 *       pair to provide the corresponding values.
 *
 *     * The `select` statement allows specifying either a list of columns or using the `*` wildcard.
 *       WHERE conditions support operators like: `>`, `<`, `<=`, `>=`, `=` and can be combined with
 *       logical operators like `and`, `or`.
 *
 *         Examples:
 *
 *             select * from TableName
 *             select column column1 from TableName
 *
 *             select column column1
 *             from TableName
 *             where column >= 1 and column <= 5
 *
 *     * The `update` statement allows modifying existing data in the database. It supports WHERE conditions
 *       in the same format as `select`.
 *
 *         Example:
 *             update TableName
 *             set column1 = newVal column2 = newVal2
 *             where column3 = 1 or column3 = 5
 *
 *     * Tables can be removed using the `drop` statement:
 *
 *         drop table TableName
 *
 *     * It is possible to load a state from .sql file
 *
 *          Example:
 *               load path
 *
 *     * it is possible to save a state to file .txt ( if during the execution user did not use `save`, during program
 *       termination there will be a request to provide a path for saving the state. If `save` was used, during
 *       program termination the state will be saved to the last used path provided to `save`)
 *
 *          Example:
 *                save path
 */

using namespace std;

using ColumnValue = variant<int, float, string>;  // Define possible types for columns


void printColumnValue(const ColumnValue &value) {
    // Use fmt::print to handle different types in the variant
    visit([](const auto &val) { fmt::print("{} ", val); }, value);
}


std::string toString(const ColumnValue &val) {
    return visit([](auto &&arg) -> std::string {
        ostringstream oss;
        oss << arg;
        return oss.str();
    }, val);
}


struct ForeignKey {
    string referencingTable;
    vector<std::string> referencingColumns;

    string referencedTable;
    vector<std::string> referencedColumns;
};

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
    vector<ForeignKey> foreignKeys;

    string savingPath;
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


void deleteTable(string &tableName, Tables<int> &tables) {
    tables.tables.erase(tableName);
}

namespace DBCommands {
    const string create = "create";
    const string insert = "insert";
    const string select = "select";
    const string where = "where";
    const string alter = "alter";
    const string foreign = "foreign";
    const string add = "add";
    const string drop = "drop";
    const string load = "load";
    const string save = "save";
    const string update = "update";
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

auto processWhereStatement(const vector<string> &query) {
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


            wherePattern.conditions.push_back(WhereCondition(currentColumn, currentOp, currentValue));

            // Reset for the next condition
            currentColumn.clear();
            currentOp.clear();
            currentValue.clear();
        }
    }

    return wherePattern;
}

void processSelect(const vector<string> &query, Tables<int> &tables) {
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

    auto &columns = tables.tables[tableName].rowColumn;

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
        for (const auto &pair: columns) {
            actualColumnsToPrint.push_back(pair.first);
        }
    } else {
        for (const auto &target: targetedColumns) {
            if (!columns.contains(target)) {
                fmt::println("No such column '{}' in table '{}'", target, tableName);
                return;
            }
            actualColumnsToPrint.push_back(target);
        }
    }

    // Print header
    for (const auto &colName: actualColumnsToPrint) {
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
    for (int rowIdx = 1; rowIdx < numRows; ++rowIdx) {
        bool conditionPass = true;  // Assume the row is valid until proven otherwise
        bool hasPassedAnyCondition = false;  // For OR logic

        // Evaluate WHERE conditions
        if (isWherePresent) {
            for (size_t i = 0; i < pattern.conditions.size(); ++i) {
                const auto &condition = pattern.conditions[i];
                const auto &columnData = columns.at(condition.column);
                const auto &cell = columnData[rowIdx];

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
                    const std::string &cellValue = std::get<std::string>(cell);

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
            for (const auto &colName: actualColumnsToPrint) {
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

    // --- Foreign key check ---
    for (const auto &fk: tables.foreignKeys) {
        if (fk.referencingTable != tableName) continue;

        vector<string> referencingValues;
        for (const auto &col: fk.referencingColumns) {
            referencingValues.push_back(columnsToValue[col]);
        }

        const auto &refTable = tables.tables[fk.referencedTable];
        int rowCount = refTable.rowColumn.begin()->second.size();
        bool matchFound = false;

        for (int row = 0; row < rowCount; ++row) {
            vector<string> referencedValues;
            for (const auto &refCol: fk.referencedColumns) {
                referencedValues.push_back(toString(refTable.rowColumn.at(refCol)[row]));
            }
            if (referencingValues == referencedValues) {
                matchFound = true;
                break;
            }
        }

        if (!matchFound) {
            fmt::println("Foreign key constraint failed: referencing values not found in referenced table '{}'.",
                         fk.referencedTable);
            return;
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

auto defineNumberOfUpdateStatements(vector<string> query) {


}


auto processUpdate(const vector<string> &query, Tables<int> &tables) {
    bool isWherePresent = false;
    auto tableName = query[1];
    WherePattern pattern;
    if (query[0] != DBCommands::update || query.size() < 3 || query[2] != "set") {
        fmt::println("invalid update Format");
        return;
    }

    if (!tables.tables.contains(tableName)) {
        fmt::println("no such table exist");
    }

    int i = 3;
    map<string, string> columnAndValue;
    for (i; i < query.size(); i++) {
        if (query[i] == DBCommands::where) {
            isWherePresent = true;
            pattern = processWhereStatement(query);
            break;
        }
        if (query[i] == "=") {
            columnAndValue[query[i - 1]] = query[i + 1];
        }
    }

    for (const auto &item: columnAndValue) {
        if (!tables.tables[tableName].rowColumn.contains(item.first)) {
            fmt::println("no such column in table {} ", tableName);
        }
    }


    if (!isWherePresent) {
        for (const auto &item: columnAndValue) {
            vector<ColumnValue> newValues(tables.tables[tableName].rowColumn[item.first].size(), item.second);
            tables.tables[tableName].rowColumn[item.first] = newValues;
        }
    } else {
        RowColumn<int> &table = tables.tables[tableName];
        int numRows = table.rowColumn.begin()->second.size();

        for (int rowIdx = 0; rowIdx < numRows; ++rowIdx) {
            bool conditionPass = true;
            bool hasPassedAnyCondition = false;

            if (isWherePresent) {
                for (size_t condIdx = 0; condIdx < pattern.conditions.size(); ++condIdx) {
                    const auto &cond = pattern.conditions[condIdx];
                    const auto &column = table.rowColumn.at(cond.column);
                    const auto &cell = column[rowIdx];
                    bool currentConditionPass = false;

                    if (std::holds_alternative<int>(cell)) {
                        int value = std::get<int>(cell);
                        int target = std::stoi(cond.value);
                        if (cond.operation == ">") currentConditionPass = value > target;
                        else if (cond.operation == "<") currentConditionPass = value < target;
                        else if (cond.operation == ">=") currentConditionPass = value >= target;
                        else if (cond.operation == "<=") currentConditionPass = value <= target;
                        else if (cond.operation == "=") currentConditionPass = value == target;
                    } else if (std::holds_alternative<std::string>(cell)) {
                        const string &value = std::get<std::string>(cell);
                        if (cond.operation == "=") currentConditionPass = value == cond.value;
                        else if (cond.operation == ">") currentConditionPass = value > cond.value;
                        else if (cond.operation == "<") currentConditionPass = value < cond.value;
                        else if (cond.operation == ">=") currentConditionPass = value >= cond.value;
                        else if (cond.operation == "<=") currentConditionPass = value <= cond.value;
                    }

                    if (condIdx > 0) {
                        const string &logic = pattern.logicalOperators[condIdx - 1];
                        if (logic == "and") {
                            conditionPass = conditionPass && currentConditionPass;
                        } else if (logic == "or") {
                            conditionPass = conditionPass || currentConditionPass;
                        }
                    } else {
                        conditionPass = currentConditionPass;
                    }
                }
            }

            if (!isWherePresent || conditionPass) {
                for (const auto &[col, strVal]: columnAndValue) {
                    auto &column = table.rowColumn[col];
                    if (std::holds_alternative<int>(column[rowIdx])) {
                        column[rowIdx] = std::stoi(strVal);
                    } else if (std::holds_alternative<float>(column[rowIdx])) {
                        column[rowIdx] = std::stof(strVal);
                    } else {
                        column[rowIdx] = strVal;
                    }
                }
            }
        }

    }

}

void processAdd(const vector<string> &query, Tables<int> &tables, const string &tableName) {
    auto newColumnName = query[4];
    auto &columns = tables.tables[tableName].rowColumn;
    auto type = query[5];
    ColumnValue defaultType;

    if (columns.contains(newColumnName)) {
        fmt::println("column {} already existst in table {} ", newColumnName, tableName);
        return;
    }

    if (type == "int") {
        // Store a vector of ColumnValue with a default value (int)
        defaultType = {ColumnValue(0)};  // Correct for vector of ColumnValue
    } else if (type == "string") {
        // Store a vector of ColumnValue with a default value (empty string)
        defaultType = {ColumnValue("null")};  // Correct for vector of ColumnValue
    } else if (type == "float") {
        // Store a vector of ColumnValue with a default value (float)
        defaultType = {ColumnValue(0.0f)};  // Correct for vector of ColumnValue
    }

    // we need to know the size of one of the primary key columns to populate the table with default values
    auto primaryKeyColumn = tables.primaryKeys[tableName][0];
    auto size = columns[primaryKeyColumn].size();

    vector<ColumnValue> defaultColumnValues(size, defaultType);

    columns[newColumnName] = defaultColumnValues;

}

void processForeignKey(const vector<string> &query, Tables<int> &tables, const string &tableName) {
    vector<string> referencingColumns;
    vector<string> referencedColumns;


    string referencedTable;

    bool inReferencingColumns = false;
    bool inReferencedColumns = false;

    for (size_t i = 0; i < query.size(); ++i) {
        if (query[i] == "foreign" && i + 1 < query.size() && query[i + 1] == "key") {
            inReferencingColumns = true;
            i += 2;
            continue;
        }

        if (inReferencingColumns && query[i] == "(") continue;
        if (inReferencingColumns && query[i] == ")") {
            inReferencingColumns = false;
            continue;
        }
        if (inReferencingColumns) {
            referencingColumns.push_back(query[i]);
            continue;
        }

        if (query[i] == "references" && i + 1 < query.size()) {
            referencedTable = query[++i];
            inReferencedColumns = true;
            continue;
        }

        if (inReferencedColumns && query[i] == "(") continue;
        if (inReferencedColumns && query[i] == ")") {
            inReferencedColumns = false;
            continue;
        }
        if (inReferencedColumns) {
            referencedColumns.push_back(query[i]);
        }
    }

    // Check that types of referencing and referenced columns match
    for (size_t i = 0; i < referencingColumns.size(); ++i) {
        const auto &referencingCol = tables.tables[tableName].rowColumn[referencingColumns[i]];
        const auto &referencedCol = tables.tables[referencedTable].rowColumn[referencedColumns[i]];

        if (!referencingCol.empty() && !referencedCol.empty()) {
            if (referencingCol.front().index() != referencedCol.front().index()) {
                fmt::println(
                        "Type mismatch: referencing column '{}' and referenced column '{}' must be of the same type.",
                        referencingColumns[i], referencedColumns[i]);
                return;
            }
        }
    }


    for (const auto &fk: tables.foreignKeys) {
        if (fk.referencingTable == tableName &&
            fk.referencedTable == referencedTable &&
            fk.referencingColumns == referencingColumns &&
            fk.referencedColumns == referencedColumns) {
            fmt::println("This foreign key relationship already exists.");
            return;
        }
    }

    //  Check existence of referenced table
    if (!tables.tables.contains(referencedTable)) {
        fmt::println("Table '{}' does not exist.", referencedTable);
        return;
    }

    //  Check if referenced columns exist in referenced table
    for (const auto &col: referencedColumns) {
        if (!tables.tables[referencedTable].rowColumn.contains(col)) {
            fmt::println("Referenced column '{}' does not exist in table '{}'.", col, referencedTable);
            return;
        }
    }

    //  Check if referencing columns exist in referencing table
    for (const auto &col: referencingColumns) {
        if (!tables.tables[tableName].rowColumn.contains(col)) {
            fmt::println("Referencing column '{}' does not exist in table '{}'.", col, tableName);
            return;
        }
    }

    // : Check if referenced columns match the primary key of the referenced table
    auto pkOfReferenced = tables.primaryKeys[referencedTable];
    vector<string> sortedReferenced = referencedColumns;
    vector<string> sortedPK = pkOfReferenced;

    sort(sortedReferenced.begin(), sortedReferenced.end());
    sort(sortedPK.begin(), sortedPK.end());

    if (sortedReferenced != sortedPK) {
        fmt::println("Referenced columns do not match the primary key of '{}'.", referencedTable);
        return;
    }

    // Step 5: Ensure sizes match (number of columns)
    if (referencingColumns.size() != referencedColumns.size()) {
        fmt::println("Mismatched column count in referencing and referenced keys.");
        return;
    }

    // Step 6: Save the foreign key definition
    ForeignKey foreignKey = ForeignKey(tableName, referencingColumns, referencedTable, referencedColumns);
    tables.foreignKeys.push_back(foreignKey);

}

auto defineNumberOfAlterStatements(const vector<string> &query) {
    vector<vector<string>> result;
    vector<string>::const_iterator beginRange;
    vector<string>::const_iterator endRange;

    bool isAlterStatement = false;

    for (auto it = query.begin(); it != query.end(); ++it) {
        if (*it == "alter" && (it + 1 != query.end()) && *(it + 1) == "table") {
            beginRange = it;
            isAlterStatement = true;
        }

        if (isAlterStatement && (*it == "drop" || *it == "add" || *it == "foreign")) {

            auto tempIt = it;
            while (tempIt != query.end() && *tempIt != "alter") {
                ++tempIt;
            }
            endRange = tempIt;

            result.emplace_back(beginRange, endRange);
            it = --tempIt;
            isAlterStatement = false;
        }
    }

    return result;
}

void alterTableDropColumn(const vector<string> &query, Tables<int> &tables, const string &tableName) {

    string columnToDrop = query[4];


    if (!tables.tables.contains(tableName)) {
        fmt::println("Table '{}' does not exist.", tableName);
        return;
    }

    RowColumn<int> &table = tables.tables[tableName];


    if (!table.rowColumn.contains(columnToDrop)) {
        fmt::println("Column '{}' does not exist in table '{}'.", columnToDrop, tableName);
        return;
    }


    auto &pkCols = tables.primaryKeys[tableName];
    if (find(pkCols.begin(), pkCols.end(), columnToDrop) != pkCols.end()) {
        fmt::println("Cannot drop column '{}': it is part of the primary key.", columnToDrop);
        return;
    }


    for (const auto &fk: tables.foreignKeys) {
        if ((fk.referencingTable == tableName &&
             find(fk.referencingColumns.begin(), fk.referencingColumns.end(), columnToDrop) !=
             fk.referencingColumns.end()) ||
            (fk.referencedTable == tableName &&
             find(fk.referencedColumns.begin(), fk.referencedColumns.end(), columnToDrop) !=
             fk.referencedColumns.end())) {
            fmt::println("Cannot drop column '{}': it is part of a foreign key relationship.", columnToDrop);
            return;
        }
    }


    table.rowColumn.erase(columnToDrop);
    fmt::println("Column '{}' dropped from table '{}'.", columnToDrop, tableName);
}

void dropTable(const vector<string> &query, Tables<int> &tables) {
    auto tableName = query[2];

    if (query.size() < 3 || query[0] != "drop" || query[1] != "table") {
        fmt::println("Incorrect drop table syntax. Expected: DROP TABLE <tableName>");
        return;
    }

    if (!tables.tables.contains(tableName)) {
        fmt::println("Table '{}' does not exist.", tableName);
        return;
    }


    deleteTable(tableName, tables);
    tables.primaryKeys.erase(tableName);

    erase_if(tables.foreignKeys, [&tableName](ForeignKey key) {
        return key.referencedTable == tableName || key.referencingTable == tableName;
    });


    fmt::println("Table '{}' dropped successfully.", tableName);
}


void processAlter(const vector<string> &query, Tables<int> &tables) {
    auto tableName = query[2];

    if (!tables.tables.contains(tableName)) {
        fmt::println("Table '{}' does not exist.", tableName);
        return;
    }

    auto table = tables.tables[tableName].rowColumn;

    //start processing from index 3
    for (int i = 3; i < query.size(); i++) {
        if (query[i] == DBCommands::add) {
            processAdd(query, tables, tableName);
        }
        if (query[i] == DBCommands::drop) {
            alterTableDropColumn(query, tables, tableName);
        }
        if (query[i] == DBCommands::foreign) {
            processForeignKey(query, tables, tableName);
        }
    }

}


void processFile(const vector<string> &query, Tables<int> &tables) {
    auto path = query[1];
    auto file = fstream(path);
    string word = "";
    vector<string> toExecute;

    if (!filesystem::exists(path)) {
        fmt::println("no such file exists");
        return;
    }


    while (file >> word) {
        toExecute.push_back(word);
    }

    auto vectorOfCreateQueries = defineNumberOfCreateStatements(toExecute);

    for (const auto &item: vectorOfCreateQueries) {
        processCreate(item, tables);
    }


    auto vectorOfInsertQueries = defineNumberOfInsertStatements(toExecute);

    for (auto &item: vectorOfInsertQueries) {
        processInsert(item, tables);
    }

    auto vectorOfAlterStatements = defineNumberOfAlterStatements(toExecute);

    for (const auto &item: vectorOfAlterStatements) {
        processAlter(item, tables);
    }


    fmt::println("{}", toExecute);

}


void processSave(const string &filePath, Tables<int> &tables) {
    std::ofstream out(filePath);  // overwrite the file
    if (!out.is_open()) {
        fmt::println("Could not open file '{}'", filePath);
        return;
    }
    tables.savingPath = filePath;

    for (const auto &[tableName, rowColumn]: tables.tables) {
        const auto &columns = rowColumn.rowColumn;
        if (columns.empty()) continue;

        out << "Table: " << tableName << "\n";

        std::vector<std::string> actualColumnsToPrint;
        for (const auto &[colName, _]: columns) {
            actualColumnsToPrint.push_back(colName);
        }

        // Header
        for (const auto &colName: actualColumnsToPrint) {
            out << fmt::format("| {:15} ", colName);
        }
        out << "|\n";

        // Separator
        for (size_t i = 0; i < actualColumnsToPrint.size(); ++i) {
            out << fmt::format("|{:-^17}", "");
        }
        out << "|\n";

        // Row count
        int numRows = columns.begin()->second.size();

        // Rows
        for (int rowIdx = 0; rowIdx < numRows; ++rowIdx) {
            for (const auto &colName: actualColumnsToPrint) {
                const auto &cell = columns.at(colName)[rowIdx];
                std::string value = std::visit([](auto &&v) { return fmt::format("{}", v); }, cell);
                out << fmt::format("| {:15} ", value);
            }
            out << "|\n";
        }

        out << "\n";
    }

    fmt::println("All tables saved to '{}'", filePath);
}


void processQuery(vector<string> query, Tables<int> &tables) {
    if (query[0] == "exit") {
        if (tables.savingPath == "") {
            fmt::println("provide a path for back up");
            string path;
            cin >> path;
            processSave(path, tables);
            fmt::println("program terminated, back up is created");
            exit(0);
        } else {
            processSave(tables.savingPath, tables);
            fmt::println("program terminated, back up is created");
            exit(0);
        }

    }

    if (query[0] == DBCommands::update) {
        processUpdate(query, tables);
        return;
    }

    if (query[0] == DBCommands::load) {
        processFile(query, tables);
        return;
    }

    if (query[0] == DBCommands::save) {
        processSave(query[1], tables);
    }

    if (query[0] == DBCommands::drop) {
        dropTable(query, tables);
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


    auto vectorOfAlterStatements = defineNumberOfAlterStatements(query);

    for (const auto &item: vectorOfAlterStatements) {
        processAlter(item, tables);
    }


    auto vectorOfInsertQueries = defineNumberOfInsertStatements(query);

    for (auto &item: vectorOfInsertQueries) {
        processInsert(item, tables);
    }


    return;
}


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
    cout << "program stopped\n";
}


int main() {
    startProgram();
}
