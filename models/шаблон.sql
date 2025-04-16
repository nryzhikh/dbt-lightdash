SELECT
    week,
    {{ roundv(tv('o.ots_all'), 1000) }} AS тв_1000_ots,
    {{ roundv(olv('показы'), 1000) }} AS олв_1000_показов,
    {{ roundv(audio('показы'), 1000) }} AS аудио_1000_показов,
    {{ roundv(indoor('o.ots_all'), 1000) }} AS индор_1000_ots,
    {{ roundv(radio('o.ots_all'), 1000) }} AS радио_1000_ots,
    {{ roundv(seeding('показы'), 1000) }} AS посевы_1000_показов,
    {{ roundv(banner('показы'), 1000) }} AS баннеры_1000_показов,
    {{ roundv(blogger('показы'), 1000) }} AS блогеры_1000_показов,
    {{ roundv(smart_tv('показы'), 1000) }} AS smart_tv_reach,
    {{ roundv(smm('охват'), 1000) }} AS ooh_1000_показов,
    {{ roundv(performance('клики', ['епк', 'epk']), 1000) }} AS перформанс_епк_клики,
    {{ roundv(performance('показы', ['сети']), 1000) }} AS перформанс_сети_1000_показов,
    {{ roundv(ooh('показы'), 1000) }} AS наружная_реклама_1000_показов,
    {{ roundv(performance('клики', ['поиск', 'search'])) }} AS перформанс_поиск_клики,
    {{ roundv(performance('показы', ['мастер', 'master']), 1000) }} AS перформанс_мастер_1000_показов,
    {{ roundv(performance('показы', ['соцсети', 'social']), 1000) }} AS перформанс_соцсети_1000_показов,
    {{ roundv(performance('клики', ['мобил', 'mobile']), 1000) }} AS перформанс_мобильный_клики,
    {{ roundv(fin_aggr('показы'), 1000) }} AS фин_агр_1000_показов,
    {{ roundv(performance('показы', ['ремаркетинг', 'remarketing']), 1000) }} AS перформанс_ремаркетинг_1000_показов,
    {{ roundv(performance('показы', ['регион', 'region']), 1000) }} AS региональный_перф_1000_показов
FROM
    {{ ref('эконометрика_общая') }} b
LEFT JOIN
    {{ ref('ots_all') }} o ON CAST(b.id AS TEXT) = o.id
GROUP BY
    week