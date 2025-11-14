CREATE OR REPLACE FUNCTION prometheus.fn_cre_liquidador_v1(p_nu_valornegocio numeric, p_in_numcuotas int4, p_dt_fechaitem date, p_vc_tipo_cuota varchar, p_vc_dpto varchar, p_dt_fecha_system date, p_vc_compra_cartera varchar, p_in_ini_cuota int4, p_vc_tipo_simulacion varchar, p_in_numero_sol int4, p_vc_nit_empresa_fondo varchar, p_in_id_producto_fondo int4, p_in_id_cobertura_fondo int4, p_unidad_negocio varchar, p_vc_nit_afiliado varchar, p_vc_id_cliente varchar, p_in_convenio int4)
	RETURNS SETOF tp_cre_liquidador_negocio
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
	
	
        /**********************************************************************************************************************
        * Liquidador de negocios general 
        * @autor : Ing. Edgar Gonzalez M.
        * @Descripcion : Realiza la liquidacion de un credito
        * @Fecha creacion : 2022-01-12
        * @Parametros:
        * 	=> p_nu_valornegocio    	: valor del credito a liquidar
        *  => p_in_numcuotas 			: # de peridos en meses a financiar la solicitud
        *  => p_dt_fechaitem 			: Fecha del primer pago
        *  => p_vc_tipo_cuota 			: tipo de liquidacion por defecto CTFCPV (cuota fija)
        *  => p_vc_dpto 				: Departamento de la ubicacion de la agencia
        *  => p_dt_fecha_system 		: fecha base para la liquidacion por defecto current_date
        *  => p_vc_compra_cartera 		: define si es compra cartera para temas de tasas
        *  => p_in_ini_cuota 			: cuotas inicial para la generacion de plan de pagos por defecto 0 
        *  => p_vc_tipo_simulacion 	: define tres tipos : (LIQUIDAR, SIMULAR,REFINANCIAR)
        *  => p_in_numero_sol 			: numero de de formulario para uso en la reliquidacion y creacion de negocio. 
        *  => p_vc_nit_empresa_fondo 	: nit de la empresa avalista para el calculo del aval
        *  => p_in_id_producto_fondo 	: producto del aval utilizado para el calculo del mismo
        *  => p_in_id_cobertura_fondo 	: cobertura del producto de aval
        *  => p_unidad_negocio         : Unidad de negocio (MICROCREDITO,EDUCATIVO,LIBRANZA)
        * 	=> p_nit_afiliado           : nit afiliado puede ser establecimiento de comercio o pagaduria
        *  => p_id_cliente             : numero de indentificacion del cliente
        *  => p_in_convenio            : identifica el convenio del credito
        *****************************************************************************************************************/
                            
    DECLARE
    
        retorno prometheus.tp_cre_liquidador_negocio;
        v_nn_salario_minimo numeric;
        v_vc_idconvenio varchar;	
        v_re_convenio record;
        v_re_impuesto record;
        v_vc_configuracionFondo varchar;
        v_id_producto_fondo int4  := p_in_id_producto_fondo;
        v_id_cobertura_fondo int4 := p_in_id_cobertura_fondo;
        v_nu_id_nit_fondo varchar := p_vc_nit_empresa_fondo;
        v_bo_flag boolean:=true;
        v_re_valorFianzaAval record;
        v_bo_financia_aval boolean ;
        v_nu_valorAvalFinanciado numeric :=0;
        v_vc_identificador varchar :='';
        v_in_key int4;
        v_nu_valorCuota numeric     := 0;
        v_nu_capital    numeric     := 0;
           
        v_dt_fechaAnterior date     := p_dt_fecha_system::date;
        v_nu_seguroItem   numeric   := 0;
        v_nu_saldoinicial numeric   := 0;
        v_nu_capacitacionItem numeric :=0;        
        
    
        v_nu_tasaInteres  	   numeric :=0;
        v_nu_tasaCAT 	  	   numeric :=0;
        v_nu_tasaImpuesto 	   numeric :=0;
        v_nu_cuota_manejo 	   numeric :=0;
        v_nu_catItem 	  	   numeric :=0;
        v_nu_cuotaAval  	   numeric :=0;
        v_nu_tic 		  	   numeric :=0;
        v_nu_estudio_credito   numeric :=0;
        v_nu_seguro_voluntario numeric :=0;
    	v_nu_porcentaje_cat_micro numeric :=0;
    
        v_dt_fechaItems date ;
        v_dt_fechaPrimerPago date :=p_dt_fechaitem ;
        v_dt_fechaAux date; --:=p_dt_fechaitem;
        v_dt_fecha_fin_mes date;
        v_in_ajuste_dia integer:=0;
    
        v_nu_interesItem numeric := 0;
        v_nu_interesItemGracia numeric :=0;
        v_nu_capitalItem numeric := 0;
        v_dias integer :=0;
        v_dt_fecha_perido_gracia date;
        v_in_diasAcomulado integer := 0;
        v_nu_saldofinal  numeric   := 0;	
        r_liq_aval record;
        v_in_numcuotas int4:=p_in_numcuotas;
        v_nu_aval_primer_mes numeric=0;
        v_nu_valornegocio numeric:=p_nu_valornegocio;
        v_in_ajuste_cuota int4:=0;
        v_vc_segmento varchar;
    	v_vc_codigo_negocio VARCHAR;
    	v_nu_monto_slmv NUMERIC;
       	v_nu_valor_interes_total NUMERIC := 0;
        v_nu_valor_cuota_calculada NUMERIC :=0;
        v_nu_valor_cuota_2 NUMERIC := 0;
       	v_vc_usuario_creacion VARCHAR;
        v_vc_identificacion VARCHAR;
    
    BEGIN
        
        --1.) Validacion de variables de entrada del liquidador
        IF p_vc_tipo_simulacion NOT IN ('LIQUIDAR','SIMULAR','REFINANCIAR') THEN 
        	RETURN ;
        END IF ;

        IF p_unidad_negocio NOT IN ('1','30','31','22','33') THEN 
        	RETURN ;
        END IF ;

        v_nn_salario_minimo:=coalesce((SELECT salario_minimo_mensual 
                                        FROM salario_minimo  
                                    where ano=substring(current_date,1,4)),0);
                            
        if v_nn_salario_minimo =0 then 
            RETURN ;
        end if;
    
        if p_unidad_negocio='1' then 
       
        	v_vc_idconvenio := p_in_convenio; 
        
            IF v_vc_idconvenio IS NULL THEN 
                v_vc_idconvenio:= (SELECT id_convenio 
                                    FROM convenios WHERE agencia=p_vc_dpto 
                                    AND id_convenio not in (37,41,15,40)  
                                    AND tipo='Microcredito' 
                                    GROUP BY id_convenio 
                                HAVING min(monto_minimo)=1);
            END IF; 
        elsif p_unidad_negocio='30' then
        	v_vc_idconvenio := p_in_convenio; 
       	elsif p_unidad_negocio='33' then
	   		v_vc_idconvenio := '69'; 
        else
        	v_vc_idconvenio:=(SELECT id_convenio 
                                FROM convenios 
                          	  WHERE agencia=p_vc_dpto 
                            	AND id_convenio not in (37,41,15,40,69)  
                            	AND unid_negocio=p_unidad_negocio);
            
        end if;
    
   		--Buscamos lo datos del convenio
        SELECT INTO v_re_convenio 
            case 
            when p_vc_compra_cartera='S'
            then tasa_compra_cartera 
            else tasa_interes 
            end as tasa_interes,
            porcentaje_cat,
            valor_capacitacion,
            valor_seguro,
            valor_central,
            cat,
            impuesto,
            tasa_compra_cartera,
            round(tasa_max_fintra/100.00::numeric,6) AS tasa_max_fintra,
            vr_estudio_credito,
            vr_tecnologia,
            cobro_cat_anticipado , 
			cobro_aval_anticipado, 
			cobro_cuota_admin_anticipado, 
			cobro_seguro_deudor_anticipado,
			cobro_tic_anticipado,
			cobro_estudio_credito_anticipado
        FROM convenios
        WHERE id_convenio= v_vc_idconvenio;
       
		IF  p_unidad_negocio='22' THEN 
	        --tasa para libranza
	        IF p_vc_nit_afiliado = '999989999' THEN ----NIT SIN PAGADURÍA PARA SIMULADOR
	        
	            SELECT c.tasa_mensual::numeric into v_nu_tasaInteres  
	            FROM configuracion_libranza c
	            INNER JOIN pagadurias p2 on p2.id=c.id_pagaduria  
	            WHERE c.id_convenio = v_vc_idconvenio
	            AND p2.documento  = p_vc_nit_afiliado ;
	        
	            v_re_convenio.tasa_interes :=v_nu_tasaInteres;
	    
	        ELSE
	        
	            SELECT c.tasa_mensual::numeric into v_nu_tasaInteres  
	                FROM configuracion_libranza c
	            INNER join pagadurias p2 on p2.id=c.id_pagaduria  
	                where c.id_convenio = v_vc_idconvenio
	                AND   c.reg_status  = '' 
	                AND   p2.reg_status = ''
	                AND   p2.documento  = p_vc_nit_afiliado ;
	                
	            v_re_convenio.tasa_interes :=v_nu_tasaInteres;
	        END IF;
       END IF;
    --   raise notice 'v_nu_tasaInteres : %',v_nu_tasaInteres;

        --Configuracion del impuestos 
        SELECT INTO v_re_impuesto
            tipo_impuesto,
            codigo_impuesto,
            porcentaje1 * ind_signo AS porcentaje1,
            porcentaje2,
            concepto,
            descripcion,
            cod_cuenta_contable
        FROM
            tipo_de_impuesto
        WHERE
            dstrct  = 'FINV'
            AND codigo_impuesto = v_re_convenio.impuesto
            AND TO_CHAR(fecha_vigencia, 'YYYY') = TO_CHAR(now(), 'YYYY');
        
           
        v_dt_fechaAux:=v_dt_fechaPrimerPago;
        
        
    	/*************************************************
        * Validacion de fecha de liquidacion de libranza 
        *************************************************/
	    if  p_unidad_negocio = '22' then 
	        v_dt_fecha_fin_mes := (TO_CHAR(DATE_TRUNC('month', (v_dt_fechaPrimerPago- INTERVAL '1 month')::date) + INTERVAL '1 month'- INTERVAL '1 day','YYYY-MM-DD')::timestamp without time ZONE)::date;
	        v_in_ajuste_dia    := (v_dt_fechaPrimerPago-v_dt_fecha_fin_mes);
	    end if; 
        
    
        /* ***************************
        * Calculo de tasas goblales *
        *****************************/
        v_nu_tasaInteres  := round((v_re_convenio.tasa_interes / 100),6);
       
      	IF p_unidad_negocio = '1' THEN 
       		
       		v_nu_monto_slmv := v_nu_valornegocio / v_nn_salario_minimo;
       	
       		SELECT porcentaje_cat
       		INTO v_nu_porcentaje_cat_micro
       		FROM public.fg_conf_cat
       		WHERE unidad_negocio = p_unidad_negocio
       	    AND v_nu_monto_slmv BETWEEN monto_minimo AND monto_maximo;
       	   
       		v_nu_tasaCAT := round((v_nu_porcentaje_cat_micro / 100),6);
       	ELSE 
       		v_nu_tasaCAT      := 0;
       	END IF;
           
        v_nu_tasaImpuesto := round((v_re_impuesto.porcentaje1 / 100 ),6);
        -- RAISE NOTICE 'interesEA : %',interesEA;

        IF(p_vc_tipo_cuota = 'CTFCPV')THEN
        
			--Calculos de cargos fijos de la liquidacion acomulados
       		v_nu_capital 			:= v_nu_valornegocio;	   
            v_nu_saldoinicial 		:= v_nu_capital;
        	
        	--1.) Aval
       		v_nu_cuotaAval :=get_valor_fianza_fondo_convenio(v_nu_valornegocio::numeric,v_in_numcuotas::int4,v_vc_idconvenio::int4,
													v_id_producto_fondo::int4,v_id_cobertura_fondo::int4,v_nu_id_nit_fondo::varchar);
        
			--2.) Ley mipyme
        	IF v_re_convenio.cat THEN
               v_nu_catItem := ROUND(((v_nu_tasaCAT * v_nu_valornegocio)* (v_nu_tasaImpuesto + 1)));
            else 
               v_nu_catItem := 0;	
            END IF;
           
            --3) Cuota de administracion o Intermediacion 
           	v_nu_cuota_manejo := coalesce((SELECT valor FROM apicredit.cuota_manejo  where id_convenio=v_vc_idconvenio and tipo_calculo='F'),0);
            v_nu_cuota_manejo := v_nu_cuota_manejo*v_in_numcuotas;
           
            RAISE NOTICE 'v_nu_saldoinicial, %', v_nu_saldoinicial;
           	RAISE NOTICE 'v_dt_fechaAnterior, %', v_dt_fechaAnterior;
           	RAISE NOTICE 'v_dt_fechaPrimerPago, %', v_dt_fechaPrimerPago;
           	RAISE NOTICE 'v_vc_idconvenio, %', v_vc_idconvenio;
           	RAISE NOTICE 'v_re_convenio.tasa_interes, %', v_re_convenio.tasa_interes;
           	RAISE NOTICE 'p_vc_tipo_simulacion, %', p_vc_tipo_simulacion;
           	RAISE NOTICE 'p_in_numero_sol, %', p_in_numero_sol;
           
           	
           
           	--4.) Seguro vida deudor 
            v_nu_seguroItem   := apicredit.seguro_vida_deudor(v_nu_saldoinicial::numeric,v_dt_fechaAnterior::date,v_dt_fechaPrimerPago::date,
           													  v_vc_idconvenio::int4,v_re_convenio.tasa_interes::NUMERIC, v_in_numcuotas::INTEGER, 
           													  p_vc_tipo_simulacion::varchar,p_in_numero_sol);  
           													 RAISE NOTICE 'Seguro, %', v_nu_seguroItem;
           	v_nu_seguroItem   :=v_nu_seguroItem*v_in_numcuotas;
           													 
			v_nu_capacitacionItem := coalesce(ROUND(v_re_convenio.valor_capacitacion),0);			
		
			v_nu_tic 			  := v_re_convenio.vr_tecnologia;
            v_nu_estudio_credito  := v_re_convenio.vr_estudio_credito;
           
           	v_nu_seguro_voluntario := prometheus.fn_get_valor_seguro_voluntario(p_in_numero_sol::integer);
            v_nu_seguro_voluntario := v_nu_seguro_voluntario*v_in_numcuotas;
           	
           	IF p_in_numero_sol > 0 AND p_vc_tipo_simulacion = 'LIQUIDAR' THEN 
           	
	           		SELECT codigo_negocio,
	           			   usuario_creacion,
	           			   identificacion 
	           		INTO v_vc_codigo_negocio,
	           			 v_vc_usuario_creacion,
	           			 v_vc_identificacion
	           		FROM prometheus.cre_solicitudes cs 
	           		WHERE numero_solicitud = p_in_numero_sol;
           	        	
                    UPDATE prometheus.cre_solicitudes 
                       SET valor_aval =v_nu_cuotaAval,
                    	valor_total_cat =v_nu_catItem,
                    	valor_total_consultores=v_nu_cuota_manejo,
                    	valor_total_seguro=v_nu_seguroItem,
                    	valor_total_tecnologia=v_nu_tic,
                    	valor_total_seguro_voluntario=v_nu_seguro_voluntario,
                    	valor_total_otro_conceptos=0,
                    	tasa = v_re_convenio.tasa_interes
                     WHERE numero_solicitud = p_in_numero_sol ;
                   
                   
               IF v_vc_codigo_negocio IS NOT NULL OR v_vc_codigo_negocio <> '' THEN 
                   UPDATE negocios
                   SET valor_aval                    = v_nu_cuotaAval,
                   	   valor_fianza                  = v_nu_cuotaAval,
                   	   valor_seguro                  = v_nu_seguroItem,
                   	   valor_total_seguro            = v_nu_seguroItem,
                   	   valor_total_cuota_admin       = v_nu_cuota_manejo,
                   	   valor_total_cat               = v_nu_catItem,
					   valor_total_tecnologia        = v_nu_tic,
					   valor_total_estudio_credito   = v_nu_estudio_credito,
					   valor_total_seguro_voluntario = v_nu_seguro_voluntario,
					   valor_total_otro_conceptos    = 0
				    WHERE cod_neg = v_vc_codigo_negocio;
			   END IF;   
			
            END IF;
           

	       	/***
	       	 * Validaciones si se financia o no los conceptos
	       	 */
        	IF v_re_convenio.cobro_aval_anticipado THEN 
        		v_nu_cuotaAval :=0;
        	ELSE 
        		v_nu_cuotaAval :=coalesce(ROUND(v_nu_cuotaAval/v_in_numcuotas),0);
        	END IF; 
        	
        	IF v_re_convenio.cobro_cat_anticipado THEN 
        		v_nu_catItem  := 0;   
        	else 
        		v_nu_catItem := coalesce(ROUND(v_nu_catItem/v_in_numcuotas),0);
        	END IF;         
        
        	IF v_re_convenio.cobro_cuota_admin_anticipado THEN 
        		v_nu_cuota_manejo :=0;
        	ELSE 
        		v_nu_cuota_manejo := coalesce(ROUND(v_nu_cuota_manejo/v_in_numcuotas),0);
        	END IF; 
        
            IF v_re_convenio.cobro_seguro_deudor_anticipado THEN 
        		v_nu_seguroItem :=0;
        	ELSE 
        		v_nu_seguroItem :=coalesce(ROUND(v_nu_seguroItem/v_in_numcuotas),0);
        	END IF; 
        
            IF v_re_convenio.cobro_tic_anticipado THEN 
        		v_nu_tic :=0;
        	ELSE 
        		v_nu_tic:=coalesce(round(v_nu_tic/v_in_numcuotas),0);
        	END IF; 
        
            IF v_re_convenio.cobro_estudio_credito_anticipado THEN 
        		v_nu_estudio_credito :=0;
        	ELSE 
        		v_nu_estudio_credito  :=  coalesce(round(v_nu_estudio_credito/v_in_numcuotas),0); 
        	END IF; 
        
        	v_nu_seguro_voluntario:=coalesce(round(v_nu_seguro_voluntario/v_in_numcuotas),0); 
        
        	
        
           --empanada de la cuota doble de microcredito
            IF p_unidad_negocio = '1' and v_vc_idconvenio not in (66,67,68) then 
            	 --v_in_numcuotas  := v_in_numcuotas+1;
            	 v_in_numcuotas  := p_in_numcuotas;
			     IF p_vc_tipo_simulacion IN ('REFINANCIAR') THEN 
			        v_in_numcuotas  := p_in_numcuotas;
			     END IF;           
			     v_nu_valorCuota := round(v_nu_capital *(power(1 + v_nu_tasaInteres, v_in_numcuotas)*v_nu_tasaInteres)/(power((1 + v_nu_tasaInteres), v_in_numcuotas)-1));
			    -- v_in_numcuotas  := p_in_numcuotas;
	        else 
                v_nu_valorCuota	:= round(v_nu_capital *(power(1 + v_nu_tasaInteres, v_in_numcuotas)*v_nu_tasaInteres)/(power((1 + v_nu_tasaInteres), v_in_numcuotas)-1));
            end if;
        
            v_dt_fecha_perido_gracia := v_dt_fechaPrimerPago-('1 month'::INTERVAL)-(v_in_ajuste_dia||' day')::interval;	    
            v_dias 					 := v_dt_fecha_perido_gracia-p_dt_fecha_system;
            v_in_diasAcomulado       := v_in_diasAcomulado + v_dias;
        
        
        --  raise notice 'p_dt_fechaitem: % v_dt_fecha_perido_gracia: % v_dias_gracia : % v_in_ajuste_dia: % v_in_diasAcomulado: %',p_dt_fechaitem,v_dt_fecha_perido_gracia,v_dias,v_in_ajuste_dia,v_in_diasAcomulado;  
            if v_dias > 0 or p_unidad_negocio = '31' then 
                v_nu_interesItem  	   :=round(v_nu_saldoinicial*v_nu_tasaInteres);
                v_nu_interesItemGracia :=round(v_dias*(v_nu_interesItem/30));
                v_dt_fechaAnterior     :=v_dt_fecha_perido_gracia;
               
            
               --Si se cqambia el que va anicipar la cuota de manejo se daña
                if p_unidad_negocio IN ('22') then 
                    v_nu_interesItemGracia:=round(round((v_nu_interesItemGracia*1.19))/v_in_numcuotas,0);
                    v_nu_cuota_manejo:=v_nu_cuota_manejo+v_nu_interesItemGracia;
                    v_nu_interesItemGracia :=0;
                    v_in_diasAcomulado:=0;
                end if; 
            
                --Plexa
                if p_unidad_negocio IN ('30') and p_in_convenio =64  then 
                retorno.interes :=0;
                v_nu_cuotaAval  :=0;
                end if;
                --LIBRANZA PUBLICA 
		        IF p_unidad_negocio IN ('33') THEN
				    v_nu_interesItemGracia :=0;
				    v_in_diasAcomulado     :=0;
				END IF; 
            
            end if; 
        
            retorno:= null;
            v_dias:=0;	    
           
            for i IN 1..v_in_numcuotas loop
                IF( i = 1)THEN
                v_dt_fechaItems := v_dt_fechaPrimerPago ;
                v_dias := (v_dt_fechaItems - v_dt_fechaAnterior)-v_in_ajuste_dia; 
                ELSE
                v_dt_fechaItems := v_dt_fechaAux + INTERVAL '1 month';
                v_dt_fechaAux   := v_dt_fechaItems;
                v_dias := (v_dt_fechaItems - v_dt_fechaAnterior); 
                END IF;
            
                if p_unidad_negocio  in ('30') and p_in_convenio in (63,64)  then 			
                    v_in_diasAcomulado := v_in_diasAcomulado + v_dias ;		
                    v_nu_interesItem   := round(v_nu_saldoinicial*v_nu_tasaInteres)+v_nu_interesItemGracia;
                    v_nu_capitalItem   := v_nu_valorCuota-v_nu_interesItem;
                    v_nu_saldofinal    := v_nu_saldoinicial-v_nu_capitalItem;
                    v_nu_interesItemGracia:=0;
                else 
                    v_in_diasAcomulado := v_in_diasAcomulado + v_dias ;		
                    v_nu_interesItem   := round(v_nu_saldoinicial*v_nu_tasaInteres);
                    v_nu_capitalItem   := v_nu_valorCuota-v_nu_interesItem;
                    v_nu_saldofinal    := v_nu_saldoinicial-v_nu_capitalItem;
                
                    --raise notice 'v_nu_saldoinicial : % v_nu_tasaInteres: % v_nu_interesItem : %', v_nu_saldoinicial,v_nu_tasaInteres,v_nu_interesItem;
                end if;
                
                --Interes primera cuota. 
                if p_unidad_negocio  in ('1') then 
                    v_nu_interesItem := v_nu_interesItem+v_nu_interesItemGracia;
                end if;
                
            
        
                if i = v_in_numcuotas then 
                    v_nu_capitalItem   :=v_nu_saldoinicial;
                    v_nu_saldofinal    :=v_nu_saldoinicial-v_nu_capitalItem;
                end if; 
                
                --financiacicion del aval
                SELECT INTO r_liq_aval * 
                FROM tem.liquidacion_aval_temporal 
                WHERE key_ref=v_vc_identificador 
                AND cuota=(i+p_in_ini_cuota) AND fecha=v_dt_fechaItems;
                    
                --Dias primera cuota por linea
                if p_unidad_negocio  in ('1') and i > 1 and v_in_diasAcomulado >30 then 
                v_in_diasAcomulado:=30;
                end if;
            
                if p_unidad_negocio  in ('22','30') and v_in_diasAcomulado >30  then 			   
                    v_in_diasAcomulado:=30;
                end if;
            
                if p_unidad_negocio  in ('31') then 
                v_in_diasAcomulado:=30;	     		      		      		      
                end if;
            
               
                v_nu_valor_cuota_calculada := v_nu_capitalItem+v_nu_interesItem+v_nu_catItem+v_nu_cuota_manejo+v_nu_seguroItem+v_nu_capacitacionItem+COALESCE(r_liq_aval.valor,0.00) +v_nu_cuotaAval+v_nu_estudio_credito+v_nu_tic+v_nu_seguro_voluntario;
               	IF  i = 2 THEN 
            		 v_nu_valor_cuota_2 := v_nu_valor_cuota_calculada;
            	END IF;
                                
                /* *********************************************************
                * Seteamos valores de la liquidacion y retornamos la fila  *
                ************************************************************/
                retorno.dias		          := v_in_diasAcomulado;
                retorno.item			      := i+p_in_ini_cuota+v_in_ajuste_cuota ;
                retorno.fecha			      := v_dt_fechaItems::date;
                retorno.saldo_inicial	      := v_nu_saldoinicial;		
                retorno.capital 		      := v_nu_capitalItem;
                retorno.interes 		      := v_nu_interesItem; 
                retorno.cat 			      := v_nu_catItem;
                retorno.cuota_manejo		  := v_nu_cuota_manejo;
                retorno.seguro 				  := v_nu_seguroItem ;
                retorno.capacitacion    	  := v_nu_capacitacionItem;
                retorno.aval				  := v_nu_cuotaAval;
                retorno.valor_aval            := COALESCE(r_liq_aval.valor,0.00);
                retorno.capital_aval          := COALESCE(r_liq_aval.capital,0.00);
                retorno.interes_aval          := COALESCE(r_liq_aval.interes,0.00);
                retorno.vr_estudio_credito    := v_nu_estudio_credito;
                retorno.vr_tecnologia		  := v_nu_tic;
                retorno.seguro_voluntario 	  := v_nu_seguro_voluntario;
                retorno.valor 		  	      := v_nu_valor_cuota_calculada;
                retorno.saldo_final 	      := v_nu_saldofinal ;
                retorno.fecha_pago_lib 	      := v_dt_fechaItems::date-v_in_ajuste_dia;
                retorno.custodia 			  := 0;
                retorno.remesa				  := 0;
                v_nu_saldoinicial		      := v_nu_saldofinal;
                v_nu_interesItemGracia        := 0;
                v_in_diasAcomulado            := 0;
                v_dt_fechaAnterior            := v_dt_fechaItems;
               
                v_nu_valor_interes_total := v_nu_valor_interes_total + v_nu_interesItem;
              
                RETURN NEXT retorno;
               
            END LOOP;
           
             --ACTUALIZAMOS EL VALOR DEL INTERES EN LA SOLICITUD Y EL NEGOCIO
            IF p_in_numero_sol > 0 AND p_vc_tipo_simulacion = 'LIQUIDAR' THEN 
	           	
                UPDATE prometheus.cre_solicitudes 
            	   SET valor_total_intereses_corrientes = COALESCE(v_nu_valor_interes_total,0),
            	   	   valor_cuota = v_nu_valor_cuota_2,
            	   	   fecha_primera_cuota = v_dt_fechaPrimerPago
                 WHERE numero_solicitud = p_in_numero_sol;
               
                --actualizamos la oferta libranza                
                IF (p_unidad_negocio IN ('22')) THEN      
         			--LIBRANZA PRIVADA 	
					PERFORM * 
					   FROM prometheus.sp_cre_calcular_detalle_oferta(p_in_numero_sol::int4, 
					   												  v_vc_usuario_creacion::varchar) 
					     AS t(resultado VARCHAR, "numeroSolicitud" VARCHAR);

                ELSIF (p_unidad_negocio IN ('33')) THEN 
                	--LIBRANZA PÚBLICA
                	PERFORM * 
                	   FROM prometheus.sp_cre_detalle_oferta_libranza_publica(p_in_numero_sol::INT4,
                															  v_vc_identificacion::VARCHAR,
                															  v_vc_usuario_creacion::VARCHAR) AS resultado;
                END IF;
                               
                IF v_vc_codigo_negocio IS NOT NULL OR v_vc_codigo_negocio <> '' THEN 
                
                   UPDATE negocios 
                      SET valor_total_intereses = COALESCE(v_nu_valor_interes_total,0)
                    WHERE cod_neg = v_vc_codigo_negocio;
                  
                END IF;                   
	                   
            END IF;
              
        END IF;
       
    END; 







$$
;

