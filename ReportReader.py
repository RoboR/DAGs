import os
import sys
from collections import namedtuple


RESULT_DATA_REPRESENTATION = \
    ['HEADER_LINE',
     'FMEFT_HEADER', 'FMHEFT_APPLICATION_HEADER', 'FMHEFT_SUMMARY',
     'WPMEFT_HEADER', 'WPMHEFT_APPLICATION_HEADER', 'WPMHEFT_SUMMARY',
     'PPMEFT_HEADER', 'PPMHEFT_APPLICATION_HEADER', 'PPMHEFT_SUMMARY',
     'NEXT_DATA_LINE']


class ResultData:
    AppResult = namedtuple('AppResult',
                           ['id', 'priority', 'nodes_no', 'lowerbound', 'deadline', 'makespan', 'lateness'])
    MHEFTResult = namedtuple('MHEFTResult',
                             ['dmr', 'overall_lateness', 'total_makespan'])

    def __init__(self):
        self.comm_ratio = 0
        self.fat_ratio = 0
        self.application_no = 0
        self.processor_no = 0
        self.deadline_ratio = 0

        self.fheft_applications = list()
        self.wpheft_applications = list()
        self.ppheft_applications = list()

        self.fheft_result = None
        self.wpheft_result = None
        self.ppmheft_result = None


def convert_line_to_app_result(line_str):
    app_data = line_str.split(';')
    app_id = app_data[0].split(':')[1].strip()
    app_priority = int(app_data[1].split(':')[1].strip())
    nodes = int(app_data[2].split(':')[1].strip())
    lowerbound = int(app_data[3].split(':')[1].strip())
    deadline = int(app_data[4].split(':')[1].strip())
    makespan = int(app_data[5].split(':')[1].strip())
    lateness = float(app_data[6].split(':')[1].strip())

    app_result = ResultData.AppResult(app_id, app_priority, nodes, lowerbound, deadline, makespan, lateness)
    return app_result


if __name__ == '__main__':
    print("Reading report")

    RESULT_DATA_SIZE = len(RESULT_DATA_REPRESENTATION)
    result_data_line_index = 0

    currentResult = None

    FILTER_FAT_RATIO_LIST = ["fat_1.0", "fat_2.0", "fat_3.0"]
    FILTER_APPLICATION_NO_LIST = [2, 3, 4, 6, 8, 10, 15, 20]
    FILTER_PROCESSOR_NO_LIST = [2, 3, 4, 8, 10]
    FILTER_DEADLINE_RATIO_LIST = [5, 10, 20, 30, 40, 50]

    for get_fat_ratio in FILTER_FAT_RATIO_LIST:
        for get_app_num in FILTER_APPLICATION_NO_LIST:
            for get_procc_num in FILTER_PROCESSOR_NO_LIST:
                for get_deadline_num in FILTER_DEADLINE_RATIO_LIST:
                    report_file = os.getcwd() + '/report_03.txt'
                    fp = open(report_file, 'r')

                    title = get_fat_ratio + '\t' \
                            + str(get_app_num) + '\t' \
                            + str(get_procc_num) + '\t' \
                            + str(get_deadline_num)
                    # title = "FAT RATIO : " + get_fat_ratio + '\t' \
                    #         "APPLICATION NO : " + str(get_app_num) + '\t' \
                    #         "PROCESSOR NO : " + str(get_procc_num) + '\t' \
                    #         "DEADLINE RATIO : " + str(get_deadline_num)
                    # print(title)

                    while True:
                        line = fp.readline()
                        if not line:
                            break

                        if RESULT_DATA_REPRESENTATION[result_data_line_index] == 'HEADER_LINE':
                            currentResult = ResultData()
                            header_data = line.split(';')
                            comm_ratio = header_data[0].split(':')[1].strip()
                            fat_ratio = header_data[1].split(':')[1].strip()
                            application_no = header_data[2].split(':')[1].strip()
                            processor_no = header_data[3].split(':')[1].strip()
                            deadline_ratio = header_data[4].split(':')[1].strip()

                            currentResult.comm_ratio = comm_ratio
                            currentResult.fat_ratio = fat_ratio
                            currentResult.application_no = int(application_no)
                            currentResult.processor_no = int(processor_no)
                            currentResult.deadline_ratio = int(deadline_ratio)

                        elif RESULT_DATA_REPRESENTATION[result_data_line_index] == 'FMEFT_HEADER' or \
                             RESULT_DATA_REPRESENTATION[result_data_line_index] == 'WPMEFT_HEADER' or \
                             RESULT_DATA_REPRESENTATION[result_data_line_index] == 'PPMEFT_HEADER':
                             do_nothing = line

                        elif RESULT_DATA_REPRESENTATION[result_data_line_index] == 'FMHEFT_SUMMARY':
                            fheft_summary_data = line.split(';')
                            dmr = float(fheft_summary_data[0].split(':')[1].strip())
                            overal_lateness = float(fheft_summary_data[1].split(':')[1].strip())
                            total_makespan = int(fheft_summary_data[2].split(':')[1].strip())
                            currentResult.fheft_result = ResultData.MHEFTResult(dmr, overal_lateness, total_makespan)

                        elif RESULT_DATA_REPRESENTATION[result_data_line_index] == 'WPMHEFT_SUMMARY':
                            fheft_summary_data = line.split(';')
                            dmr = float(fheft_summary_data[0].split(':')[1].strip())
                            overal_lateness = float(fheft_summary_data[1].split(':')[1].strip())
                            total_makespan = int(fheft_summary_data[2].split(':')[1].strip())
                            currentResult.wpheft_result = ResultData.MHEFTResult(dmr, overal_lateness, total_makespan)

                        elif RESULT_DATA_REPRESENTATION[result_data_line_index] == 'PPMHEFT_SUMMARY':
                            fheft_summary_data = line.split(';')
                            dmr = float(fheft_summary_data[0].split(':')[1].strip())
                            overal_lateness = float(fheft_summary_data[1].split(':')[1].strip())
                            total_makespan = int(fheft_summary_data[2].split(':')[1].strip())
                            currentResult.ppmheft_result = ResultData.MHEFTResult(dmr, overal_lateness, total_makespan)

                        elif RESULT_DATA_REPRESENTATION[result_data_line_index] == 'FMHEFT_APPLICATION_HEADER':
                            app_remain_to_read = int(line.split(':')[1].strip())

                            for app in range(app_remain_to_read):
                                app_line = fp.readline().strip()
                                res = convert_line_to_app_result(app_line)
                                currentResult.fheft_applications.append(res)

                        elif RESULT_DATA_REPRESENTATION[result_data_line_index] == 'WPMHEFT_APPLICATION_HEADER':
                            app_remain_to_read = int(line.split(':')[1].strip())

                            for app in range(app_remain_to_read):
                                app_line = fp.readline().strip()
                                convert_line_to_app_result(app_line)
                                currentResult.wpheft_applications.append(res)

                        elif RESULT_DATA_REPRESENTATION[result_data_line_index] == 'PPMHEFT_APPLICATION_HEADER':
                            app_remain_to_read = int(line.split(':')[1].strip())

                            for app in range(app_remain_to_read):
                                app_line = fp.readline().strip()
                                convert_line_to_app_result(app_line)
                                currentResult.ppheft_applications.append(res)

                        elif RESULT_DATA_REPRESENTATION[result_data_line_index] == 'NEXT_DATA_LINE':
                            result = title + "\t"
                            if currentResult.fat_ratio == get_fat_ratio and \
                               currentResult.application_no == get_app_num and \
                               currentResult.processor_no == get_procc_num and \
                               currentResult.deadline_ratio == get_deadline_num:
                                fmheft_lateness_min = 65535
                                fmheft_lateness_max = 0
                                fmheft_lateness_avg = 0
                                wpmheft_lateness_min = 65535
                                wpmheft_lateness_max = 0
                                wpmheft_lateness_avg = 0
                                ppmheft_lateness_min = 65535
                                ppmheft_lateness_max = 0
                                ppmheft_lateness_avg = 0

                                for fmheft in currentResult.fheft_applications:
                                    if fmheft.lateness < fmheft_lateness_min:
                                        fmheft_lateness_min = fmheft.lateness
                                    if fmheft.lateness > fmheft_lateness_max:
                                        fmheft_lateness_max = fmheft.lateness
                                    fmheft_lateness_avg += fmheft.lateness

                                for wpmheft in currentResult.wpheft_applications:
                                    if wpmheft.lateness < wpmheft_lateness_min:
                                        wpmheft_lateness_min = wpmheft.lateness
                                    if wpmheft.lateness > wpmheft_lateness_max:
                                        wpmheft_lateness_max = wpmheft.lateness
                                    wpmheft_lateness_avg += wpmheft.lateness

                                for ppmheft in currentResult.ppheft_applications:
                                    if ppmheft.lateness < ppmheft_lateness_min:
                                        ppmheft_lateness_min = ppmheft.lateness
                                    if ppmheft.lateness > ppmheft_lateness_max:
                                        ppmheft_lateness_max = ppmheft.lateness
                                    ppmheft_lateness_avg += ppmheft.lateness

                                fmheft_lateness_avg = float(fmheft_lateness_avg) / float(currentResult.application_no)
                                wpmheft_lateness_avg = float(wpmheft_lateness_avg) / float(currentResult.application_no)
                                ppmheft_lateness_avg = float(ppmheft_lateness_avg) / float(currentResult.application_no)

                                # DMR
                                result += str(currentResult.fheft_result.dmr) + '\t'
                                result += str(currentResult.wpheft_result.dmr) + '\t'
                                result += str(currentResult.ppmheft_result.dmr) + '\t'

                                # make span
                                result += str(currentResult.fheft_result.total_makespan) + '\t'
                                result += str(currentResult.wpheft_result.total_makespan) + '\t'
                                result += str(currentResult.ppmheft_result.total_makespan) + '\t'

                                # overall lateness
                                result += str(currentResult.fheft_result.overall_lateness) + '\t'
                                result += str(currentResult.wpheft_result.overall_lateness) + '\t'
                                result += str(currentResult.ppmheft_result.overall_lateness) + '\t'

                                result += str(fmheft_lateness_min) + '\t'
                                result += str(fmheft_lateness_max) + '\t'
                                result += str(fmheft_lateness_avg) + '\t'
                                result += str(wpmheft_lateness_min) + '\t'
                                result += str(wpmheft_lateness_max) + '\t'
                                result += str(wpmheft_lateness_avg) + '\t'
                                result += str(ppmheft_lateness_min) + '\t'
                                result += str(ppmheft_lateness_max) + '\t'
                                result += str(ppmheft_lateness_avg) + '\t'
                                print(result)

                        result_data_line_index = (result_data_line_index + 1) % RESULT_DATA_SIZE

                    fp.close()
