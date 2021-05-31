import os
import json
import pandas as pd

from pyecharts import options as opts
from pyecharts.charts import Bar, Map, WordCloud, Line
from pyecharts.render import make_snapshot
from pyecharts.globals import ThemeType

from snapshot_selenium import snapshot


def read_csv(path):
    for filename in os.listdir(path):
        if filename.endswith(".csv"):
            df = pd.read_csv(os.path.join(path, filename))
            return df
    return None


'''
# 平均薪资水平柱状图
df = read_csv("salary_average")
graph = (
    Bar(init_opts=opts.InitOpts(theme=ThemeType.LIGHT))
    .add_xaxis(df["job_category"].values.tolist())
    .add_yaxis("平均最低月薪", df["min_salary_average"].values.tolist())
    .add_yaxis("平均最高月薪", df["max_salary_average"].values.tolist())
    .set_global_opts(
        title_opts=opts.TitleOpts(title="平均月薪", subtitle="单位: 千元"),
        xaxis_opts=opts.AxisOpts(
            axislabel_opts=opts.LabelOpts(interval=0, font_size=8)
        )
    )
)
make_snapshot(snapshot, graph.render(), "salary_average_month.png")

graph = (
    Bar()
    .add_xaxis(df["job_category"].values.tolist())
    .add_yaxis("平均年薪", df["year_salary_average"].values.tolist())
    .set_global_opts(
        title_opts=opts.TitleOpts(title="平均年薪", subtitle="单位: 万元"),
        xaxis_opts=opts.AxisOpts(
            axislabel_opts=opts.LabelOpts(interval=0, font_size=8)
        )
    )
)
make_snapshot(snapshot, graph.render(), "salary_year.png")

# 统计岗位数量
df = read_csv("job_number_count_by_category")
graph = (
    Bar(init_opts=opts.InitOpts(theme=ThemeType.WALDEN))
    .add_xaxis(df["job_category"].values.tolist())
    .add_yaxis("岗位数量", df["count"].values.tolist())
    .set_global_opts(title_opts=opts.TitleOpts(title="不同专业领域招聘岗位数量"),
                     xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(interval=0, font_size=8)))
)
make_snapshot(snapshot, graph.render(), "job_number_by_category.png")

# 统计不同地区岗位数量分布
df = read_csv("job_area")
area = df["_1"].values.tolist()
number = df["_2"].values.tolist()
result = list()
for i in range(0, len(area)):
    result.append((area[i], number[i]))

graph = (
    Map()
    .add("岗位数量", result, "厦门", 
         label_opts=opts.LabelOpts(formatter='{b}\n{c}'))
    .set_global_opts(
        title_opts=opts.TitleOpts(title="招聘数量地区分布图"),
        visualmap_opts=opts.VisualMapOpts(
            max_=6000,
            is_piecewise=True,
            range_color=["lightskyblue", "yellow", "orangered"])
    )
)
make_snapshot(snapshot, graph.render(), "job_number_by_area.png")

# 统计关键词词云图
df = read_csv("keyword_popularity")
results = []
key = df["_1"].values.tolist()
value = df["_2"].values.tolist()
for i in range(0, len(key)):
    results.append((key[i], value[i]))
graph = (
    WordCloud()
    .add(series_name="关键词热度", data_pair=results, word_size_range=[6, 66])
    .set_global_opts(title_opts=opts.TitleOpts(
        title="关键词热度词云", title_textstyle_opts=opts.TextStyleOpts(font_size=23)
    ), tooltip_opts=opts.TooltipOpts(is_show=True))
)
make_snapshot(snapshot, graph.render(), "keyword_cloud.png")

# 公司规模与岗位数量
df = read_csv("job_number_count")
graph = (
    Bar(init_opts=opts.InitOpts(theme=ThemeType.WONDERLAND))
    .add_xaxis(df["company_name"].head(15).values.tolist())
    .add_yaxis("工作机会数量", df["count"].head(15).values.tolist())
    .set_global_opts(title_opts=opts.TitleOpts(title="岗位数量"),
                     xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(interval=0, font_size=8)))
)
make_snapshot(snapshot, graph.render(), "job_count_scale.png")

# 经验与学历需求相关统计
experience = read_csv("experience_salary")
graph_average = (
    Line()
    .add_xaxis(experience["experience_requirement"].values.tolist())
    .add_yaxis("平均薪资 (单位：千元)", experience["salary_average"].values.tolist(), yaxis_index=1, z=100)

)
graph_count = (
    Bar(init_opts=opts.InitOpts(theme=ThemeType.WALDEN))
    .add_xaxis(experience["experience_requirement"].values.tolist())
    .add_yaxis("岗位数量", experience["count"].values.tolist())
    .extend_axis(yaxis=opts.AxisOpts(interval=2.5))
    .set_global_opts(
        title_opts=opts.TitleOpts(title="工作经验与薪资的关系"),
        yaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(formatter="{value}")),
    )
)
graph_count.overlap(graph_average)
graph_count.render()

education = read_csv("education_salary")
graph_average = (
    Line()
    .add_xaxis(education["education_requirement"].values.tolist())
    .add_yaxis("平均薪资 (单位：千元)", education["salary_average"].values.tolist(), yaxis_index=1, z=100)
)
graph_count = (
    Bar(init_opts=opts.InitOpts(theme=ThemeType.LIGHT))
    .add_xaxis(education["education_requirement"].values.tolist())
    .add_yaxis("岗位数量", education["count"].values.tolist())
    .extend_axis(yaxis=opts.AxisOpts(interval=2.5))
    .set_global_opts(
        title_opts=opts.TitleOpts(title="学历要求与薪资的关系"),
        yaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(formatter="{value}")),
    )
)
graph_count.overlap(graph_average)
graph_count.render()
'''

df = read_csv("experience_with_scale")
experience = ["经验不限", "在校/应届", "1年以内", "1-3年", "3-5年", "5-10年", "10年以上"]
graph = Bar(init_opts=opts.InitOpts(theme=ThemeType.WALDEN))\
    .add_xaxis(df["company_scale"].unique().tolist())
for item in experience:
    graph.add_yaxis(item, df.loc[df["experience_requirement"] == item]["count"].values.tolist(), stack="1")

graph.reversal_axis().set_series_opts(label_opts=opts.LabelOpts(position="right", is_show=False))
graph.render()
# make_snapshot(snapshot, graph.render(), "educationith_scale.png")

'''
df = read_csv("education_with_scale")
educations = ["初中及以下", "中专/中技", "高中", "大专", "本科", "硕士", "博士", "学历不限"]
graph = Bar(init_opts=opts.InitOpts(theme=ThemeType.WALDEN))\
    .add_xaxis(df["company_scale"].unique().tolist())
for item in educations:
    graph.add_yaxis(item, df.loc[df["education_requirement"] == item]["count"].values.tolist(), stack="1")

graph.reversal_axis().set_series_opts(label_opts=opts.LabelOpts(position="right", is_show=False))
make_snapshot(snapshot, graph.render(), "educationith_scale.png")
'''