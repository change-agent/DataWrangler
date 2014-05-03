/* Data wrangler
 *
 * Skeleton program written by Ben Rubinstein, April 2014
 *
 * Modifications by Daniel Masters (583334), May 2014
 */

#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>

#define COLS 100	/* max number of columns of data */
#define ROWS 1000	/* max number of rows of data */
#define LINELEN	1000	/* max input line length */
#define MAXPROJECT 100	/* max number of columns to project onto */
#define TRUE 1
#define FALSE 0

#define DATADELIM ","	/* column delimeter for record input/output */

#define ERROR (-1)	/* error return value from some functions */

#define ADD		'a'	/* command to add a record to data */
#define PRINT		'?'	/* command to print the data */
#define PROJECT		'p'	/* command to project data */
#define ALLCOMMANDS	"a?p"	/* list of all commands */

typedef int record_t[COLS];	/* one element of data */
typedef record_t data_t[ROWS];	/* a dataset of records */

/****************************************************************/

/* function prototypes */

void print_prompt(void);
void copy_record(record_t dest, record_t src, int len);
void print_record(record_t record, int record_len);
void print_data(data_t data, int rows, int cols);
int read_line(char *line, int maxlen);
int parse_integers(char *str, char *delim, int results[], int max_results, 
				   int first_line);
void process_line(data_t data, int *rows, int *cols, char *line, 
				  int *first_line);
void do_add(record_t record, data_t data, int *rows, int cols);
void do_project(data_t src, data_t dest, int rows, int cols,
                int target[], int target_len);

/****************************************************************/

/* orchestrate the entire program
 */
int
main(int argc, char *argv[]) {
	int cols = 0, rows = 0, first_line = TRUE;
	data_t data;
	char line[LINELEN+1];
	
	/* prompt for commands by-the-line:
	 * read the lines, process them; repeat.
	 */
	print_prompt();
	while (read_line(line, LINELEN)) {
		process_line(data, &rows, &cols, line, &first_line);
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
	if (record_len == 0) {
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
read_line(char *line, int maxlen) {
	int n = 0;
	int oversize = 0;
	int c;
	while (((c=getchar())!=EOF) && (c!='\n')) {
		if (n < maxlen) {
			if (!isspace(c)) {
				line[n++] = c;
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
			   int first_line){

	int num_results = 0;
	int num;
	char *token;
	token = strtok(str, delim);
	while (token != NULL) {
		if ((num=atoi(token)) == 0) {
			return ERROR;
		}
		if (num_results < max_results || first_line) {
			results[num_results] = num;
		}
		num_results++;
		token = strtok(NULL, delim);
	}
	return num_results;
}

/****************************************************************/

/* parse a line of input into the components of a command;
 * call appropriate function to execute command
 */
void
process_line(data_t data, int *rows, int *cols, char *line, int *first_line) {
	data_t temp_data;
	record_t record;
	int columns[MAXPROJECT];
	int comtype, i, len;

	/* do nothing on a NULL or empty line */
	if (!line || strlen(line) == 0) {
		return;
	}
	
	/* the command type is given by the first char
	 * make sure it is valid
	 */
	comtype = line[0];
	if (strchr(ALLCOMMANDS, comtype) == NULL) {
		printf("Unknown command \'%c\'\n", comtype);
		return;
	}

	/* drill down to command specifics: specific parsing, execution */
	if (comtype == ADD) {
		if (*first_line){
			*cols = parse_integers(line+1, DATADELIM, record, *cols, 
								   *first_line);
			if (*cols>COLS) {
				printf("Input was %d columns - must be %d\n", *cols, COLS);
				exit(EXIT_FAILURE);
			}
			do_add(record, data, rows, *cols);
			*first_line = FALSE;
			return;
		}
		len = parse_integers(line+1, DATADELIM, record, *cols, *first_line);
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
		len = parse_integers(line+1, DATADELIM, columns,
		                     MAXPROJECT, *first_line);
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
		do_project(data, temp_data, *rows, *cols, columns, len);
		print_data(temp_data, *rows, len);
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
	printf("Rows: %d\n", *rows);
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
			dest[i][k] = src[i][target[j] - 1];
		}
	}
}

