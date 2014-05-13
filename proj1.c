/* Data wrangler
 *
 * Skeleton program written by Ben Rubinstein, April 2014
 *
 * Modifications by Daniel Benjamin Masters (583334), May 2014
 */

#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>

#define COLS 		100		/* max number of columns of data */
#define ROWS 		1000	/* max number of rows of data */
#define LINELEN		1000	/* max input line length */
#define MAXPROJECT 	100		/* max number of columns to project onto */
#define TRUE 		1
#define FALSE 		0
#define LESSTHAN    60      /* ASCII code for "<" */
#define GREATERTHAN 62      /* ASCII code for ">" */
#define EQUALTO     61      /* ASCII code for "=" */

#define DATADELIM ","		/* column delimeter for record input/output */
#define SEL_OPERATORS "<>=" /*operators that Select command accepts */

#define ERROR (-1)			/* error return value from some functions */

#define ADD			'a'		/* command to add a record to data */
#define PRINT		'?'		/* command to print the data */
#define PROJECT		'p'		/* command to project data */
#define SELECT 		's' 	/* command to select data */
#define ALLCOMMANDS	"a?ps"	/* list of all commands */
#define COMPCOMMAND "ps"	/* list of all composition commands */

typedef int record_t[COLS];		/* one element of data */
typedef record_t data_t[ROWS];	/* a dataset of records */

/****************************************************************/

/* function prototypes */

void print_prompt(void);
void copy_record(record_t dest, record_t src, int len);
void print_record(record_t record, int record_len);
void print_data(data_t data, int rows, int cols);
int read_line(char *line, int maxlen, int *pipe_count);
int parse_integers(char *str, char *delim, int results[], int max_results, 
				   int first_line, int num_results);
int parse_operator(char *str, char *operator, int args[]);
void process_line(data_t data, int *rows, int *cols, char *line, 
				  int *first_line, int *pipe_count, int *first_pipe, 
				  int *mid_pipe, int *last_pipe, int *pipe, int *num_results);
void do_add(record_t record, data_t data, int *rows, int cols);
void do_project(data_t src, data_t dest, int rows, int cols, int target[], 
				int target_len);
void do_select(data_t src, data_t dest, int rows, int col_index, int select_arg, 
		  	   int target_len, int operator);

void save_select(data_t src, data_t set, int target_len, int i);

/****************************************************************/

/* orchestrate the entire program
 */
int
main(int argc, char *argv[]) {
	int cols = 0, rows = 0, first_line = TRUE, pipe_count = 0, i=0,
		first_pipe = FALSE, mid_pipe = FALSE, last_pipe = FALSE, pipe=0,
		num_results = 0;
	data_t data;
	char line[LINELEN+1];
	
	/* prompt for commands by-the-line:
	 * read the lines, process them; repeat.
	 */
	print_prompt();
	while (read_line(line, LINELEN, &pipe_count)) {
		if (pipe_count>0){
			printf("pipe_count: %d\n", pipe_count);
			while (i < pipe_count){
				printf("main\n");
				process_line(data, &rows, &cols, line, &first_line, 
							 &pipe_count, &first_pipe, &mid_pipe,
							 &last_pipe, &pipe, &num_results);
				i++;
				printf("i: %d\n", i);
				printf("pipe_count: %d\n\n", pipe_count);
				process_line(data, &rows, &cols, line, &first_line, 
							 &pipe_count, &first_pipe, &mid_pipe,
							 &last_pipe, &pipe, &num_results);
				pipe_count=0;
				i=0;
			}
		} else {
			process_line(data, &rows, &cols, line, &first_line, &pipe_count, 
						 &first_pipe, &mid_pipe, &last_pipe, &pipe, 
						 &num_results);
		}
		print_prompt();
	}

	/* all done */
	printf("\n");
	return 0;
}

/****************************************************************/

/* prompt user to enter for more input
 */
void
print_prompt(void) {
	printf("> ");
}

/****************************************************************/

/* copy contents of one record to another
 */
void
copy_record(record_t dest, record_t src, int len) {
	int i;
	for (i=0; i<len; i++) {
		dest[i] = src[i];
	}
}

/****************************************************************/

/* print a record
 */
void
print_record(record_t record, int record_len) {
	int i = 0;

	/* Added the "or" condition here to ensure empty rows from do_select are not
	printed */
	if (record_len == 0 || record[0]==0) {
		return;
	}
	printf("%d", record[i++]);
	while (i<record_len) {
		printf("%s %d", DATADELIM, record[i++]);
	}
	printf("\n");
}

/****************************************************************/

/* print an entire dataset
 */
void
print_data(data_t data, int rows, int cols) {
	int i;
	if (rows <= 0 || cols <= 0) {
		printf("Empty data.\n");
		return;
	}
	for (i=0; i<rows; i++) {
		print_record(data[i], cols);
	}
}

/****************************************************************/

/* read in a line of input, strip space
 */
int
read_line(char *line, int maxlen, int *pipe_count) {
	int n = 0;
	int oversize = 0;
	int c;
	while (((c=getchar())!=EOF) && (c!='\n')) {
		if (n < maxlen) {
			if (!isspace(c)) {
				line[n++] = c;
			}
			if (c=='|'){
				++*pipe_count;
			}
		}
		else {
			oversize++;
		}
	}
	line[n] = '\0';
	if (oversize > 0) {
		printf("Warning! %d over limit. Line truncated.\n",
		       oversize);
	}
	return ((n>0) || (c!=EOF));
}

/****************************************************************/

/* parse string for a delimited-list of positive integers.
 * Returns number of ints parsed or -1 if a delimited
 * token is not a valid int. If more than max_results
 * ints are parsed, the excess will not be written to results;
 * it is recommended that the calling function notify the
 * user that some data was discarded. Note! str will be
 * modified as a side-effect of running this function: delims
 * replaced by \0
 */
int
parse_integers(char *str, char *delim, int results[], int max_results, 
			   int first_line, int num_results){

	int num;
	char *token;
	token = strtok(str, delim);
	while (token != NULL){
		if (*token != '|') {
			if ((num=atoi(token)) == 0) {
				return ERROR;
			}
			if (num_results < max_results || first_line) {
				printf("num: %d\n", num);
				results[num_results] = num;
			}
			(num_results)++;
			printf("num_resultsjj: %d\n", num_results);
			token = strtok(NULL, delim);
		}
	}	
	return num_results;
}

/****************************************************************/

/* Parse string for "select" command
 */
int
parse_operator(char *str, char *operator, int args[]){
	int num_args = 0;
	int num;
	char *token, *chosen_operation;

	token = strtok(str+num_args, operator);
	while (token != NULL) {
		if ((num=atoi(token)) == 0) {
			return ERROR;
		}
		if (num_args < 2) {
			args[num_args] = num;
			printf("%d\n", args[num_args]);
			printf("Args: %d\n", num_args);
			token = strtok(NULL, operator);
			num_args++;
		} else {
		    return ERROR;
		}
	}
	chosen_operation = strpbrk(str, operator);
	if (*chosen_operation == '<'){
	    return LESSTHAN;
	} else if (*chosen_operation == '>'){
	    return GREATERTHAN;
	} else {
	    return EQUALTO;
	}
	return 0;
}

/****************************************************************/

/* parse a line of input into the components of a command;
 * call appropriate function to execute command
 */
void
process_line(data_t data, int *rows, int *cols, char *line, int *first_line,
			 int *pipe_count, int *first_pipe, int *mid_pipe, int *last_pipe, 
			 int *pipe, int *num_results){

	data_t temp_data, pipe_temp_data;
	record_t record;
	int columns[MAXPROJECT];
	int comtype, i, operator, len;

	/* do nothing on a NULL or empty line */
	if (!line || strlen(line) == 0) {
		return;
	}
	
	/* the command type is given by the first char
	 * make sure it is valid
	 */
	comtype = line[0+*num_results];
	if (strchr(ALLCOMMANDS, comtype) == NULL) {
		printf("Unknown command \'%c\'\n", comtype);
		return;
	}
	if (*pipe_count>1){
		printf("pipe: %d\n", *pipe);
		if (*pipe==0){
			*first_pipe=TRUE;
			printf("first pip tue\n");
			(*pipe)++;
		} else if (*pipe<*pipe_count){
			*first_pipe=FALSE;
			*mid_pipe=TRUE;
			printf("mid pip true\n");
			(*pipe)++;
		} else if (*pipe==*pipe_count){
			*mid_pipe=FALSE;
			*last_pipe=TRUE;
			printf("last pip triue \n");
		}
	} else if (*pipe_count==1){
		if (*pipe==0){
			*first_pipe=TRUE;
			(*pipe)++;
		} else if (*pipe==1){
			printf("pipe==1\n");
			*first_pipe=FALSE;
			*last_pipe=TRUE;
		}
	}

	/* drill down to command specifics: specific parsing, execution */
	if (comtype == ADD) {
		if (*first_line){
			*cols=parse_integers(line+1, DATADELIM, record, *cols, 
								   *first_line, *num_results);
			if (*cols>COLS) {
				printf("Input was %d columns - must be %d\n", *cols, COLS);
				/* ~H~ MUST CHANGE THIS*/
				exit(EXIT_FAILURE);
			}
			do_add(record, data, rows, *cols);
			*first_line = FALSE;
			return;
		}
		len = parse_integers(line+1, DATADELIM, record, *cols, 
							 *first_line, *num_results);
		if (len == ERROR) {
			printf("Invalid record not added to data.\n");
			return;
		}
		if (len < *cols) {
			printf("Record's %d columns too few. Not added.\n",
			       len);
			return;
		}
		if (len > *cols) {
			printf("Record truncated from %d to %d columns.\n",
			       len, *cols);
			len = *cols;
		}
		do_add(record, data, rows, *cols);
	} else if (comtype == PRINT) {
		print_data(data, *rows, *cols);
	} else if (comtype == PROJECT) {
		if ((*rows <= 0) || (*cols <= 0)) {
			print_data(data, *rows, *cols);
			return;
		}
		if (*first_pipe){
			printf("first_pipe_parse\n");
			len = parse_integers(line+1, DATADELIM, columns,
		                     	 MAXPROJECT, *first_line, *num_results);
		} else if (*mid_pipe){
			printf("mid_pipe_parse\n");
			len = parse_integers(line+1+*num_results, DATADELIM, columns,
		                     	 MAXPROJECT, *first_line, *num_results);
		} else if (*last_pipe){
			printf("last_pipe_parse\n");
			printf("num_results: %d\n", *num_results);
			len = parse_integers(line+1+*num_results, DATADELIM, columns,
		                     	 MAXPROJECT, *first_line, *num_results);
		} else {
			len = parse_integers(line+1, DATADELIM, columns,
		                     	 MAXPROJECT, *first_line, *num_results);
		}
		if (len == ERROR) {
			printf("Invalid target columns specified.\n");
			return;
		}
		for (i=0; (i<len) && (i<MAXPROJECT); i++) {
			if (columns[i] < 1 || columns[i] > *cols) {
				printf("Invalid column %d.\n", columns[i]);
				return;
			}
		}
		if (len > MAXPROJECT) {
			printf("Projecting onto %d of %d cols specified\n",
			       MAXPROJECT, len);
			len = MAXPROJECT;
		}
		/* Check which pipe the program is up to; this ensures temp_data is 
		piped instead of normal data where necessary, and data is printed if 
		processing last pipe. */
		if (*first_pipe){
			printf("first_pipe do_p\n");
			do_project(data, temp_data, *rows, *cols, columns, len);
			return;
		}

		if (*mid_pipe){
			printf("mid do_p\n");
			do_project(temp_data, pipe_temp_data, *rows, *cols, columns, len);
			return;
		}
		
		if (*last_pipe){
			printf("last_pipe\n");
			do_project(temp_data, pipe_temp_data, *rows, *cols, columns, len);
			print_data(temp_data, *rows, len);
			*last_pipe=FALSE;
			return;
		}
		else {
			do_project(data, temp_data, *rows, *cols, columns, len);
			print_data(temp_data, *rows, len);
		}
	} else if (comtype == SELECT){
		int col_index, select_arg;
		/* Here, columns = args */

		/*operator = parse_operator(line+1, SEL_OPERATORS, columns);
		if (operator == ERROR) {
			printf("Number of arguments should be exactly 2\n");
			return;
		}*/
		col_index = columns[0];

		if (col_index>len || col_index<0){
			printf("Invalid column specified.\n");
			return;
		}

		col_index = 2;
		select_arg = 6;
		operator = LESSTHAN;
		do_select(data, temp_data, *rows, col_index, select_arg, len, operator);
		print_data(temp_data, *rows, *cols);
		
	}
	return;
}

/****************************************************************/

/* copy over a given record to the end of data
 */
void
do_add(record_t record, data_t data, int *rows, int cols) {
	if (*rows >= ROWS) {
		printf("Cannot add another record, already at limit\n");
		return;
	}
	copy_record(data[(*rows)++], record, cols);
	printf("Added record: ");
	print_record(record, cols);
}

/****************************************************************/

/* project the data onto the specified columns.
 * assumes the target columns specified are all valid.
 * column indexing begins at 1.
 */
void
do_project(data_t src, data_t dest, int rows, int cols,
           int target[], int target_len) {
	int i, j, k;
	for (i=0; i<rows; i++) {
		for (j=0, k=0; j<target_len; j++, k++) {
			printf("target: %d\n", target[j]);
			dest[i][k] = src[i][target[j]-1];
			printf("dest: %d\n", dest[i][k]);
		}
	}
}

/****************************************************************/

/* Performs the "select" operation. Column indexing begins at 1.
 */
void
do_select(data_t src, data_t dest, int rows, int col_index, int select_arg, 
		  int target_len, int operator) {

	int i;
	/*
	for (i=0; i<rows; i++) {
		for (j=0, k=0; j<target_len; j++, k++) {
			if (operator == LESSTHAN){
				if (src[i][target[j]] < args+1){
					dest[i][k] = src[i][target[j]];
				}
			}
		}
	}
	if (operator == LESSTHAN){
		for (i = 0; i < rows; i++){
			if (src[i][target[args]] < args+1){
				for (j = 0; j < cols; i++){
					
				}
				dest[i][k] = src[i][target[j]];
			}
		}
		
	}*/

	for (i=0; i < rows; i++){
		if (operator == LESSTHAN){
			if (src[i][col_index-1] < select_arg){
				save_select(src, dest, target_len, i);
			}
		} else if (operator == EQUALTO){
			if (src[i][col_index-1] > select_arg){
				save_select(src, dest, target_len, i);
			}
		} else {
			if (src[i][col_index-1] == select_arg){
				save_select(src, dest, target_len, i);
			}
		}
	}
}

void
save_select(data_t src, data_t dest, int target_len, int i){
	int j;
	for (j=0; j < target_len; j++){
		dest[i][j] = src[i][j];
	}
}
