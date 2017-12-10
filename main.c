#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>

typedef struct {
    int element_id;
    int x;
    int y;
} Message;

void register_custom_type()
{
    const int nitems=3;
    int blocklengths[3] = {1, 1, 1};
    MPI_Datatype types[3] = {MPI_INT, MPI_INT, MPI_INT};
    MPI_Datatype mpi_message_type;
    MPI_Aint offsets[3];

    offsets[0] = offsetof(Message, element_id);
    offsets[1] = offsetof(Message, x);
    offsets[0] = offsetof(Message, y);

    MPI_Type_create_struct(nitems, blocklengths, offsets, types, &mpi_message_type);
    MPI_Type_commit(&mpi_message_type);
}

void read_line_from_file(FILE *fp, Message *message)
{
    char line[50];
    fgets(line, sizeof(line), fp);
    sscanf_s(line, "%d %d %d", &message->element_id, &message->x, &message->y);
}

void insert_element(int rank, int n, Message message, int *elements)
{
    if (rank <= n && rank == message.x)
    {
        elements[message.y] = message.element_id;
        return;
    }
    if (rank > n && ((rank - n) == message.y))
    {
        elements[message.x] = message.element_id;
        return;
    }
}

int find_element(Message message, int rank, int *elements, int n)
{
    int result = 0;
    for (int i=0; i<=n; i++)
    {
        if (elements[i] == message.element_id)
        {
            if (message.x == 0)
            {
                elements[i] = 0;
            } else
            {
                result = rank % n == 0 ? n : rank % n;
            }
            break;
        }
    }

    return result;
}

void print_result(int *results, int n)
{
    int result_x = -1;
    int result_y = -1;
    for (int i=0; i<=n*2; i++)
    {
        if (results[i] > 0)
        {
            if (i <= n) {
                result_x = results[i];
            } else {
                result_y = results[i];
            }
        }
    }

    printf("%d %d\n", result_x, result_y);
}

void open_input_file(char *filename, FILE **fp)
{
    fopen_s(fp, filename, "r");
    if(fp == NULL)
    {
       fprintf(stderr, "Error opening file");
       MPI_Abort(MPI_COMM_WORLD, 1);
    }
}

int main(int argc, char *argv[])
{
    MPI_Init(NULL, NULL);

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    int world_size;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    if (world_size % 2 != 1 || world_size < 3)
    {
        fprintf(stderr, "The number of processes should be odd and greater that one!\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    /* create a type for struct Message */
    register_custom_type();

    int n = (world_size-1) / 2;
    int *elements = (int *)malloc(sizeof(int)*(n+1));
    Message message;

    FILE *fp;
    if (rank == 0)
    {
        open_input_file(argv[1], &fp);
    }

    while(1)
    {
        if (rank == 0)
        {
            read_line_from_file(fp, &message);
        }

        MPI_Barrier(MPI_COMM_WORLD);
        MPI_Bcast(&message, sizeof(Message), MPI_BYTE, 0, MPI_COMM_WORLD);

        if (message.element_id < 0 || message.x < 0 || message.y < 0)
        {
           break;
        }
        if (message.element_id <= 0)
        {
            if (rank == 0)
            {
                printf("0 0\n");
            }
            continue;
        }

        int result = 0;
        if (rank != 0)
        {
            if (message.x == 0 || message.y == 0)
            {
                result = find_element(message, rank, elements, n);
            } else
            {
                insert_element(rank, n, message, elements);
            }
        }

        int *results = NULL;
        if (rank == 0) {
          results = (int *)malloc(sizeof(int) * (world_size - 1));
        }

        MPI_Barrier(MPI_COMM_WORLD);
        MPI_Gather(&result, 1, MPI_INT, results, 1, MPI_INT, 0, MPI_COMM_WORLD);

        if (rank == 0)
        {
            if (message.y == 0)
            {
                print_result(results, n);
            } else
            {
                printf("0 0\n");
            }
        }
    }

    if (rank == 0)
    {
        printf("GAME OVER!\n");
        fclose(fp);
    }

    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Finalize();
    return 0;
}
